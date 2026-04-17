import argparse
import re
from pathlib import Path

import pandas as pd

# =========================
# CONFIGURÁVEL (contas EXATAS)
# =========================
BLOCO_1 = {331, 332, 333, 34}   # somar
BLOCO_2 = {441, 442}           # subtrair


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
            .apply(lambda s: sorted(set(s.dropna().tolist()))[:5])
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
            .apply(lambda s: sorted(set(s.dropna().tolist()))[:5])
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
    - Bloco 1 (somar) = soma(BLOCO_1)
    - Bloco 2 (subtrair) = soma(BLOCO_2)
    - AUX_RES_O_BRU = (Bloco 1) - (Bloco 2)
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
        df_q[df_q["CD_CONTA_CONTABIL"].isin(BLOCO_1)]
          .groupby("REGISTRO_OPERADORA")["Diferenca"]
          .sum()
          .rename("BLOCO_1")
    )

    bloco2 = (
        df_q[df_q["CD_CONTA_CONTABIL"].isin(BLOCO_2)]
          .groupby("REGISTRO_OPERADORA")["Diferenca"]
          .sum()
          .rename("BLOCO_2")
    )

    out = meta.join(bloco1, how="left").join(bloco2, how="left")
    out["BLOCO_1"] = out["BLOCO_1"].fillna(0)
    out["BLOCO_2"] = out["BLOCO_2"].fillna(0)

    out["AUX_RES_O_BRU"] = out["BLOCO_1"] - out["BLOCO_2"]

    out = out.reset_index()
    out = out[
    ]
