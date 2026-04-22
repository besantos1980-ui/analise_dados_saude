import requests
from io import BytesIO
import pandas as pd
import zipfile
from datetime import datetime
import re

# -------------------------
# Utilitários de limpeza
# -------------------------

def only_digits_str(x):
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\.0$", "", s)
    s = re.sub(r"\D", "", s)
    return s

def normalize_account_code(x):
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\D", "", s)
    return s

def parse_ptbr_number(x):
    if pd.isna(x):
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return 0.0
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        return float(s) if s not in ("", "-", ".", "-.") else 0.0
    except ValueError:
        return 0.0

# -------------------------
# Extração de Contábeis (Dinâmica)
# -------------------------

def processar_contabeis():
    all_chunks = []
    contas_alvo = ["314", "315", "443", "43", "46", "415", "416"]
    
    trimestres_processados = 0
    ano_atual = datetime.today().year

    # Cria uma lista de candidatos descendo a partir do ano atual até 4 anos atrás
    # Ex: (2026, 4), (2026, 3)... (2025, 4)...
    candidatos = []
    for ano in range(ano_atual, ano_atual - 5, -1):
        for t in [4, 3, 2, 1]:
            candidatos.append((ano, t))

    print("Buscando os últimos 12 trimestres disponíveis na ANS...")

    for ano, t in candidatos:
        # Interrompe o loop assim que conseguir 12 trimestres com sucesso
        if trimestres_processados >= 12:
            break

        trimestre = f"{t}T{ano}"
        url = f"https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/{ano}/{trimestre}.zip"

        try:
            response = requests.get(url, stream=True, timeout=180)
            
            # Se a ANS não publicou ainda (404), pula silenciosamente e vai para o trimestre anterior
            if response.status_code == 404:
                print(f"{trimestre}: Ainda não disponível (404). Testando anterior...")
                continue
                
            response.raise_for_status()

            data = BytesIO(response.content)
            with zipfile.ZipFile(data) as zf:
                csv_files = [f for f in zf.namelist() if f.lower().endswith(".csv")]
                if not csv_files:
                    print(f"Nenhum CSV no ZIP para {trimestre}.")
                    continue

                trimestre_chunks = []
                for csv_name in csv_files:
                    with zf.open(csv_name) as file:
                        for chunk in pd.read_csv(
                            file, sep=";", encoding="latin1", chunksize=100000, low_memory=False, dtype=str
                        ):
                            # Padroniza colunas
                            rename_dict = {}
                            for col in chunk.columns:
                                cu = col.upper()
                                if cu.startswith("REG_ANS"):
                                    rename_dict[col] = "REG_ANS"
                                elif cu.startswith("CD_CONTA_CONTABIL"):
                                    rename_dict[col] = "CD_CONTA_CONTABIL"
                                elif cu.startswith("VL_SALDO_INICIAL"):
                                    rename_dict[col] = "VL_SALDO_INICIAL"
                                elif cu.startswith("VL_SALDO_FINAL"):
                                    rename_dict[col] = "VL_SALDO_FINAL"

                            chunk.rename(columns=rename_dict, inplace=True)

                            required = {"REG_ANS", "CD_CONTA_CONTABIL", "VL_SALDO_INICIAL", "VL_SALDO_FINAL"}
                            if not required.issubset(set(chunk.columns)):
                                continue

                            # Normaliza
                            chunk["REG_ANS"] = chunk["REG_ANS"].apply(only_digits_str)
                            chunk["CD_CONTA_CONTABIL"] = chunk["CD_CONTA_CONTABIL"].apply(normalize_account_code)

                            # Filtra as contas alvo
                            chunk = chunk[chunk["CD_CONTA_CONTABIL"].isin(contas_alvo)].copy()
                            if chunk.empty:
                                continue

                            # Parse matemático e cálculo da diferença
                            chunk["VL_SALDO_INICIAL"] = chunk["VL_SALDO_INICIAL"].apply(parse_ptbr_number)
                            chunk["VL_SALDO_FINAL"] = chunk["VL_SALDO_FINAL"].apply(parse_ptbr_number)
                            chunk["Diferenca"] = chunk["VL_SALDO_FINAL"] - chunk["VL_SALDO_INICIAL"]
                            chunk["Trimestre"] = trimestre

                            out = chunk[["REG_ANS", "CD_CONTA_CONTABIL", "Diferenca", "Trimestre"]]
                            trimestre_chunks.append(out)

                if trimestre_chunks:
                    df_tri = pd.concat(trimestre_chunks, ignore_index=True)
                    all_chunks.append(df_tri)
                    trimestres_processados += 1
                    print(f"OK: {trimestre} processado ({trimestres_processados}/12)")
                else:
                    print(f"{trimestre}: Sem dados financeiros úteis encontrados.")

        except requests.exceptions.HTTPError as e:
            print(f"{trimestre}: erro HTTP -> {e}")
        except Exception as e:
            print(f"Erro geral no contábil {trimestre}: {e}")

    if not all_chunks:
        print("Nenhum dado contábil processado.")
        return pd.DataFrame()

    return pd.concat(all_chunks, ignore_index=True)

# -------------------------
# Principal
# -------------------------

def main():
    print("Iniciando extração dos dados contábeis (Cálculo X)...")
    df_contabeis = processar_contabeis()

    if df_contabeis.empty:
        print("Sem contábeis para processar.")
        return

    print("\nProcessamento concluído. Aplicando a fórmula X = (314+315) - (443+43+46+415+416)...")
    
    # 1. Cria a Pivot Table para transformar as contas em colunas
    df_pivot = df_contabeis.pivot_table(
        index=['REG_ANS', 'Trimestre'],
        columns='CD_CONTA_CONTABIL',
        values='Diferenca',
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    # 2. Garante que todas as contas existam como colunas
    contas_alvo = ["314", "315", "443", "43", "46", "415", "416"]
    for conta in contas_alvo:
        if conta not in df_pivot.columns:
            df_pivot[conta] = 0.0

    # 3. Aplica a fórmula solicitada
    df_pivot['X'] = (df_pivot['314'] + df_pivot['315']) - \
                    (df_pivot['443'] + df_pivot['43'] + df_pivot['46'] + df_pivot['415'] + df_pivot['416'])

    # 4. Seleciona colunas finais
    df_final = df_pivot[['REG_ANS', 'Trimestre', 'X']].copy()

    # 5. Converte REG_ANS para número estrito (Int64 evita casas decimais indesejadas)
    print("Convertendo REG_ANS para formato numérico...")
    df_final['REG_ANS'] = pd.to_numeric(df_final['REG_ANS'], errors='coerce').astype('Int64')
    
    # Remove linhas vazias residuais caso tenham ficado na conversão
    df_final.dropna(subset=['REG_ANS'], inplace=True)

    # 6. Salva o resultado
    ts = datetime.today().strftime("%d_%m_%Y")
    out_file = f"base_calculo_dinamico_X_{ts}.xlsx"
    
    df_final.to_excel(out_file, index=False)
    
    print("\n=== RESUMO ===")
    print(f"Linhas geradas no arquivo: {len(df_final)}")
    print(f"Operadoras únicas: {df_final['REG_ANS'].nunique()}")
    print(f"Arquivo salvo com sucesso: {out_file}")

if __name__ == "__main__":
    main()
