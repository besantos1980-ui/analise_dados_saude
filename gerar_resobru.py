import argparse
import re
from pathlib import Path

import pandas as pd

# =========================
# CONFIGURÁVEL (contas EXATAS)
# =========================
EVENTOS_CONTAS = {331, 332, 333, 34}   # bloco 1 (somar)
CONTA_41 = {441, 442}                 # bloco 2 (subtrair)


def sanitize_sheet_name(name: str) -> str:
    """Garante nome de aba válido no Excel (máx. 31 chars e sem caracteres proibidos)."""
    name = str(name)
    name = re.sub(r"[:\\/?*\[\]]", "_", name)
    return name[:31]


def trimestre_sort_key(tri: str):
    """
    Ordena trimestres em formato 1T2023 (ou 1T23) cronologicamente.
    Retorna (ano, trimestre). Se não casar com o padrão, joga pro final.
    """
    tri = str(tri).strip().upper()

    m4 = re.match(r"^([1-4])T(\d{4})$", tri)
    if m4:
        q = int(m4.group(1))
        year = int(m4.group(2))
        return (year, q)

    m2 = re.match(r"^([1-4])T(\d{2})$", tri)
    if m2:
        q = int(m2.group(1))
        yy = int(m2.group(2))
        year = 2000 + yy if yy <= 79 else 1900 + yy
        return (year, q)

    return (9999, 9)


def validar_consistencia(df: pd.DataFrame):
    """
    Valida se, para cada REGISTRO_OPERADORA, Nome_Fantasia e Modalidade
    são consistentes (apenas 1 valor distinto não-nulo no arquivo inteiro).
    Se houver inconsistência, levanta ValueError com detalhes.
    """
    problemas = []

    def _prep_str(s: pd.Series) -> pd.Series:
        s = s.astype("string").str.strip()
        s = s.replace("", pd.NA)
        return s

    # Nome_Fantasia
    nf = df[["REGISTRO_OPERADORA", "Nome_Fantasia"]].copy()
    nf["Nome_Fantasia"] = _prep_str(nf["Nome_Fantasia"])
    g_nf = (
        nf.dropna(subset=["REGISTRO_OPERADORA"])
          .groupby("REGISTRO_OPERADORA")["Nome_Fantasia"]
          .nunique(dropna=True)
    )
    inconsist_nf = g_nf[g_nf > 1].index.tolist()

    if inconsist_nf:
        amostra = (
            nf[nf["REGISTRO_OPERADORA"].isin(inconsist_nf)]
            .groupby("REGISTRO_OPERADORA")["Nome_Fantasia"]
            .apply(lambda s: sorted(set([v for v in s.dropna().tolist()]))[:5])
        )
        problemas.append(("Nome_Fantasia", amostra))

    # Modalidade
    md = df[["REGISTRO_OPERADORA", "Modalidade"]].copy()
    md["Modalidade"] = _prep_str(md["Modalidade"])
    g_md = (
        md.dropna(subset=["REGISTRO_OPERADORA"])
          .groupby("REGISTRO_OPERADORA")["Modalidade"]
          .nunique(dropna=True)
    )
    inconsist_md = g_md[g_md > 1].index.tolist()

    if inconsist_md:
        amostra = (
            md[md["REGISTRO_OPERADORA"].isin(inconsist_md)]
            .groupby("REGISTRO_OPERADORA")["Modalidade"]
            .apply(lambda s: sorted(set([v for v in s.dropna().tolist()]))[:5])
        )
        problemas.append(("Modalidade", amostra))

    if problemas:
        msg = ["❌ Inconsistência detectada por REGISTRO_OPERADORA:"]
        for campo, serie in problemas:
            msg.append(f"\nCampo: {campo}")
            for op, vals in serie.head(20).items():
                msg.append(f"  - {op}: {vals}")
            if len(serie) > 20:
                msg.append(f"  ... e mais {len(serie) - 20} operadoras")
        raise ValueError("\n".join(msg))


def build_quarter_sheet(df_q: pd.DataFrame) -> pd.DataFrame:
    """
    Para um trimestre, agrega por operadora:
    - Nome_Fantasia
    - Modalidade
    - Contas de receitas auxiliares resobru = soma(EVENTOS_CONTAS)
    - 41 = soma(CONTA_41)
    - AUX_RES_O_BRU = (bloco 1) - (bloco 2)
    """
    meta = (
        df_q.sort_values(["REGISTRO_OPERADORA"])
            .groupby("REGISTRO_OPERADORA", as_index=True)
            .agg(
                Nome_Fantasia=("Nome_Fantasia", "first"),
                Modalidade=("Modalidade", "first"),
            )
    )

    bloco1 = (
        df_q[df_q["CD_CONTA_CONTABIL"].isin(EVENTOS_CONTAS)]
          .groupby("REGISTRO_OPERADORA")["Diferenca"]
          .sum()
          .rename("Contas de receitas auxiliares resobru")
    )

    bloco2 = (
        df_q[df_q["CD_CONTA_CONTABIL"].isin(CONTA_41)]
          .groupby("REGISTRO_OPERADORA")["Diferenca"]
          .sum()
          .rename("41")
    )

    out = meta.join(bloco1, how="left").join(bloco2, how="left")
    out["Contas de receitas auxiliares resobru"] = out["Contas de receitas auxiliares resobru"].fillna(0)
    out["41"] = out["41"].fillna(0)

    # ✅ aqui é SUBTRAÇÃO
    out["AUX_RES_O_BRU"] = out["Contas de receitas auxiliares resobru"] - out["41"]

    out = out.reset_index()
    out = out[
        [
            "REGISTRO_OPERADORA",
            "Nome_Fantasia",
            "Modalidade",
            "Contas de receitas auxiliares resobru",
            "41",
            "AUX_RES_O_BRU",
        ]
    ].sort_values(["REGISTRO_OPERADORA"])

    return out


def main(input_file: str, output_file: str):
    input_path = Path(input_file)
    output_path = Path(output_file)

    if not input_path.exists():
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {input_path.resolve()}")

    df = pd.read_excel(input_path, engine="openpyxl")

    required_cols = {
        "REGISTRO_OPERADORA",
        "Nome_Fantasia",
        "Modalidade",
        "Trimestre",
        "CD_CONTA_CONTABIL",
        "Diferenca",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Colunas ausentes no Excel de entrada: {sorted(missing)}")

    # Normalizações de tipo (robustas)
    df["REGISTRO_OPERADORA"] = df["REGISTRO_OPERADORA"].astype(str).str.strip()

    # evita NaN virar "NAN"
    df["Trimestre"] = df["Trimestre"].astype("string").str.strip().str.upper()
    df = df[df["Trimestre"].notna() & (df["Trimestre"] != "")]

    df["CD_CONTA_CONTABIL"] = pd.to_numeric(df["CD_CONTA_CONTABIL"], errors="coerce").astype("Int64")
    df["Diferenca"] = pd.to_numeric(df["Diferenca"], errors="coerce").fillna(0)

    # Validação global
    validar_consistencia(df)

    # Trimestres únicos ordenados
    trimestres = sorted(df["Trimestre"].unique(), key=trimestre_sort_key)

    print("Trimestres únicos:", trimestres)
    print("Qtd trimestres:", len(trimestres))
    print("Linhas totais:", len(df))
    print("Trimestre head:", df["Trimestre"].head(10).tolist())

    if len(trimestres) == 0:
        raise ValueError("Nenhum trimestre encontrado na coluna Trimestre. Não há abas para gerar.")

    # ExcelWriter é o modo correto para gerar múltiplas abas em um único arquivo. [1](https://bing.com/search?q=GitHub+Codespaces+prebuild+billing)[2](https://medium.com/@udtc.us/understanding-the-cost-of-github-codespaces-a-deep-dive-into-2-core-instances-913a110eefb3)
    with pd.ExcelWriter(output_path, engine="openpyxl", mode="w") as writer:
        resumo_parts = []

        for tri in trimestres:
            df_q = df[df["Trimestre"].eq(tri)].copy()

