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
# Extração de Contábeis
# -------------------------

def processar_contabeis():
    anos = [2023, 2024, 2025]
    all_chunks = []

    # Contas de interesse (incluindo a 412 pedida na soma e a 414 pedida na lista)
    contas_alvo = ["311", "312", "313", "32", "411", "412", "414"]

    for ano in anos:
        for t in range(1, 5):
            trimestre = f"{t}T{ano}"
            url = f"https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/{ano}/{trimestre}.zip"

            try:
                response = requests.get(url, stream=True, timeout=180)
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
                                file,
                                sep=";",
                                encoding="latin1",
                                chunksize=100000,
                                low_memory=False,
                                dtype=str 
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

                                # Normaliza chaves
                                chunk["REG_ANS"] = chunk["REG_ANS"].apply(only_digits_str)
                                chunk["CD_CONTA_CONTABIL"] = chunk["CD_CONTA_CONTABIL"].apply(normalize_account_code)

                                # Filtro Exato
                                chunk = chunk[chunk["CD_CONTA_CONTABIL"].isin(contas_alvo)].copy()
                                if chunk.empty:
                                    continue

                                # Parse numérico robusto
                                chunk["VL_SALDO_INICIAL"] = chunk["VL_SALDO_INICIAL"].apply(parse_ptbr_number)
                                chunk["VL_SALDO_FINAL"] = chunk["VL_SALDO_FINAL"].apply(parse_ptbr_number)

                                # Cálculo da diferença do período
                                chunk["Diferenca"] = chunk["VL_SALDO_FINAL"] - chunk["VL_SALDO_INICIAL"]
                                chunk["Trimestre"] = trimestre

                                out = chunk[["REG_ANS", "CD_CONTA_CONTABIL", "Diferenca", "Trimestre"]]
                                trimestre_chunks.append(out)

                    if trimestre_chunks:
                        df_tri = pd.concat(trimestre_chunks, ignore_index=True)
                        all_chunks.append(df_tri)
                
                print(f"OK: {trimestre}")

            except requests.exceptions.HTTPError:
                if response.status_code == 404:
                    print(f"{trimestre}: arquivo não encontrado (404).")
                else:
                    print(f"{trimestre}: erro HTTP.")
            except Exception as e:
                print(f"Erro geral no contábil {trimestre}:", e)

    if not all_chunks:
        print("Nenhum dado contábil processado.")
        return pd.DataFrame()

    return pd.concat(all_chunks, ignore_index=True)

# -------------------------
# Principal 
# -------------------------

def main():
    print("Iniciando extração dos dados contábeis...")
    df_contabeis = processar_contabeis()

    if df_contabeis.empty:
        print("Sem contábeis para processar.")
        return

    print("\nProcessamento concluído. Agrupando e somando as contas por operadora e trimestre...")
    
    # 1. Cria uma Pivot Table (tabela dinâmica) para transformar as contas de linhas para colunas
    # Isso soma automaticamente se houver mais de um lançamento da mesma conta no mesmo trimestre
    df_pivot = df_contabeis.pivot_table(
        index=['REG_ANS', 'Trimestre'],
        columns='CD_CONTA_CONTABIL',
        values='Diferenca',
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    # 2. Garante que todas as contas existam como coluna (caso alguma conta não venha em nenhum trimestre)
    contas_alvo = ["311", "312", "313", "32", "411", "412", "414"]
    for conta in contas_alvo:
        if conta not in df_pivot.columns:
            df_pivot[conta] = 0.0

    # 3. Realiza as somas solicitadas
    df_pivot['Contraprestações efetivas'] = df_pivot['311'] + df_pivot['312'] + df_pivot['313'] + df_pivot['32']
    df_pivot['Eventos Líquidos'] = df_pivot['411'] + df_pivot['412']

    # 4. Filtra apenas as colunas de interesse para o arquivo final
    # A conta 414 foi incluída separadamente para não perder a informação solicitada na regra 2
    colunas_finais = [
        'REG_ANS', 
        'Trimestre', 
        'Contraprestações efetivas', 
        'Eventos Líquidos', 
        '414'
    ]
    df_final = df_pivot[colunas_finais].copy()

    # 5. Salva o resultado
    ts = datetime.today().strftime("%d_%m_%Y")
    out_file = f"base_financeira_agrupada_{ts}.xlsx"
    
    df_final.to_excel(out_file, index=False)
    
    print("\n=== RESUMO ===")
    print(f"Linhas geradas no arquivo: {len(df_final)}")
    print(f"Operadoras únicas capturadas: {df_final['REG_ANS'].nunique()}")
    print(f"Arquivo salvo com sucesso: {out_file}")

if __name__ == "__main__":
    main()
