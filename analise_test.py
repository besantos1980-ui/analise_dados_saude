
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
    """Converte para string e mantém apenas dígitos (remove espaços, .0, etc)."""
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\.0$", "", s)     # remove sufixo .0 se existir
    s = re.sub(r"\D", "", s)       # mantém só dígitos
    return s

def normalize_account_code(x):
    """
    Normaliza código de conta como string.
    - remove espaços
    - remove caracteres não-numéricos (se vier com pontuação)
    OBS: se você tiver contas com zeros à esquerda e isso for relevante,
         me diga que ajusto para preservar.
    """
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\D", "", s)
    return s

def parse_ptbr_number(x):
    """
    Parse robusto pt-BR:
    - '' -> 0
    - '1.234.567,89' -> 1234567.89
    - já numérico -> float
    """
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

def audit_stage(df, stage_name, value_col="Diferenca"):
    """Resumo rápido por etapa."""
    out = {
        "etapa": stage_name,
        "linhas": len(df),
        "soma": df[value_col].sum() if value_col in df.columns else None
    }
    if "REG_ANS" in df.columns:
        out["operadoras_unicas"] = df["REG_ANS"].nunique()
    elif "REGISTRO_OPERADORA" in df.columns:
        out["operadoras_unicas"] = df["REGISTRO_OPERADORA"].nunique()
    else:
        out["operadoras_unicas"] = None
    return out

# -------------------------
# Cadastro (CADOP)
# -------------------------

def processar_cadastro():
    url = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
    try:
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()

        df = pd.read_csv(BytesIO(response.content), sep=";", encoding="latin1", low_memory=False)

        # Renomear colunas (case-insens)
        rename_dict = {}
        for col in df.columns:
            cu = col.upper()
            if cu.startswith("REGISTRO"):
                rename_dict[col] = "REGISTRO_OPERADORA"
            elif cu.startswith("NOME_FANTASIA"):
                rename_dict[col] = "Nome_Fantasia"
            elif cu.startswith("MODALIDADE"):
                rename_dict[col] = "Modalidade"

        df.rename(columns=rename_dict, inplace=True)

        # Normaliza chave
        if "REGISTRO_OPERADORA" in df.columns:
            df["REGISTRO_OPERADORA"] = df["REGISTRO_OPERADORA"].apply(only_digits_str)

        df.to_csv("cadop_debug.csv", index=False, encoding="utf-8")
        print(f"Cadastro (ativas): {len(df)} linhas | operadoras únicas: {df['REGISTRO_OPERADORA'].nunique()}")
        return df

    except Exception as e:
        print("Erro cadastro:", e)
        return pd.DataFrame()

# -------------------------
# Contábeis
# -------------------------

def processar_contabeis():
    anos = [2023, 2024, 2025]
    all_chunks = []
    audit_rows = []

    # >>> CONTAS EXATAS (ajuste aqui)
    contas_alvo = ["31", "311", "312", "32", "41"]

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
                                dtype=str  # lê tudo como string e parseia manualmente
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

                                # >>> FILTRO EXATO
                                chunk = chunk[chunk["CD_CONTA_CONTABIL"].isin(contas_alvo)].copy()
                                if chunk.empty:
                                    continue

                                # Parse numérico robusto
                                chunk["VL_SALDO_INICIAL"] = chunk["VL_SALDO_INICIAL"].apply(parse_ptbr_number)
                                chunk["VL_SALDO_FINAL"] = chunk["VL_SALDO_FINAL"].apply(parse_ptbr_number)

                                # Não dropa linhas: vazio/ruim vira 0.0 no parser
                                chunk["Diferenca"] = chunk["VL_SALDO_FINAL"] - chunk["VL_SALDO_INICIAL"]
                                chunk["Trimestre"] = trimestre

                                out = chunk[[
                                    "REG_ANS",
                                    "CD_CONTA_CONTABIL",
                                    "VL_SALDO_INICIAL",
                                    "VL_SALDO_FINAL",
                                    "Diferenca",
                                    "Trimestre"
                                ]]

                                trimestre_chunks.append(out)

                    if trimestre_chunks:
                        df_tri = pd.concat(trimestre_chunks, ignore_index=True)
                        all_chunks.append(df_tri)
                        audit_rows.append(audit_stage(df_tri, f"contabeis_{trimestre}", value_col="Diferenca"))

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
        return pd.DataFrame(), pd.DataFrame()

    df_contabeis = pd.concat(all_chunks, ignore_index=True)
    audit_df = pd.DataFrame(audit_rows)

    # Auditoria global
    audit_total = pd.DataFrame([audit_stage(df_contabeis, "contabeis_total_filtrados", value_col="Diferenca")])
    audit_df = pd.concat([audit_df, audit_total], ignore_index=True)

    return df_contabeis, audit_df

# -------------------------
# Principal com diagnóstico
# -------------------------

def main():
    df_cadastro = processar_cadastro()
    df_contabeis, audit_contabeis = processar_contabeis()

    if df_contabeis.empty:
        print("Sem contábeis para processar.")
        return

    ts = datetime.today().strftime("%d_%m_%Y")

    if not df_cadastro.empty:
        df_cadastro_key = df_cadastro.copy()
        df_cadastro_key["REGISTRO_OPERADORA"] = df_cadastro_key["REGISTRO_OPERADORA"].apply(only_digits_str)

        # LEFT para diagnosticar quem some no CADOP (ativas)
        df_merged = pd.merge(
            df_contabeis,
            df_cadastro_key[["REGISTRO_OPERADORA", "Nome_Fantasia", "Modalidade"]],
            left_on="REG_ANS",
            right_on="REGISTRO_OPERADORA",
            how="left",
            indicator=True
        )

        sem_cadop = df_merged[df_merged["_merge"] == "left_only"].copy()
        com_cadop = df_merged[df_merged["_merge"] == "both"].copy()

        print("\n=== AUDITORIA CONTÁBEIS ===")
        print(audit_contabeis.to_string(index=False))

        print("\n=== AUDITORIA MERGE (CADOP ATIVAS) ===")
        print(f"Linhas contábeis filtradas (antes do merge): {len(df_contabeis)}")
        print(f"Operadoras únicas contábeis: {df_contabeis['REG_ANS'].nunique()}")
        print(f"Linhas SEM match no CADOP (ativas): {len(sem_cadop)}")
        print(f"Operadoras SEM match no CADOP (ativas): {sem_cadop['REG_ANS'].nunique()}")
        print(f"Soma Diferenca SEM match no CADOP (ativas): {sem_cadop['Diferenca'].sum():,.2f}")
        print(f"Linhas COM match (ativas): {len(com_cadop)}")
        print(f"Operadoras COM match (ativas): {com_cadop['REG_ANS'].nunique()}")
        print(f"Soma Diferenca COM match (ativas): {com_cadop['Diferenca'].sum():,.2f}")

        # Saídas
        out_base = f"arquivo_base_resoper_{ts}.xlsx"
        out_sem_cadop = f"operadoras_sem_cadop_{ts}.xlsx"
        out_audit = f"audit_resoper_{ts}.xlsx"

        df_final_out = com_cadop[[
            "REG_ANS",
            "REGISTRO_OPERADORA",
            "Nome_Fantasia",
            "Modalidade",
            "Trimestre",
            "CD_CONTA_CONTABIL",
            "Diferenca"
        ]].copy()
        df_final_out.rename(columns={"REG_ANS": "REGISTRO_OPERADORA_CONTABEIS"}, inplace=True)
        df_final_out.to_excel(out_base, index=False)

        if len(sem_cadop) > 0:
            sem_cadop_out = sem_cadop[["REG_ANS", "Trimestre", "CD_CONTA_CONTABIL", "Diferenca"]].copy()
            sem_cadop_out.to_excel(out_sem_cadop, index=False)

        with pd.ExcelWriter(out_audit, engine="openpyxl") as writer:
            audit_contabeis.to_excel(writer, sheet_name="aud_contabeis", index=False)
            pd.DataFrame([{
                "linhas_contabeis_filtradas": len(df_contabeis),
                "operadoras_contabeis_filtradas": df_contabeis["REG_ANS"].nunique(),
                "soma_contabeis_filtradas": df_contabeis["Diferenca"].sum(),
                "linhas_final_ativas": len(com_cadop),
                "operadoras_final_ativas": com_cadop["REG_ANS"].nunique(),
                "soma_final_ativas": com_cadop["Diferenca"].sum(),
                "linhas_sem_cadop": len(sem_cadop),
                "operadoras_sem_cadop": sem_cadop["REG_ANS"].nunique(),
                "soma_sem_cadop": sem_cadop["Diferenca"].sum()
            }]).to_excel(writer, sheet_name="aud_merge", index=False)

        print("\nArquivos gerados:")
        print(f"- Base final (ativas): {out_base}")
        if len(sem_cadop) > 0:
            print(f"- Operadoras sem CADOP (ativas): {out_sem_cadop}")
        print(f"- Auditoria: {out_audit}")

    else:
        print("Cadastro vazio — exportando contábeis filtrados sem merge.")
        df_contabeis.to_excel(f"contabeis_filtrados_{ts}.xlsx", index=False)

if __name__ == "__main__":
    main()
