import argparse
import re
from pathlib import Path

import pandas as pd

EVENTOS_CONTAS = {31, 311, 312, 32}
CONTA_41 = 41


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

    # Nome_Fantasia
    nf = (
        df[["REGISTRO_OPERADORA", "Nome_Fantasia"]]
        .dropna(subset=["REGISTRO_OPERADORA"])
        .copy()
    )
    nf["Nome_Fantasia"] = nf["Nome_Fantasia"].astype(str).str.strip()

    g_nf = nf.groupby("REGISTRO_OPERADORA")["Nome_Fantasia"].nunique(dropna=True)
    inconsist_nf = g_nf[g_nf > 1].index.tolist()

    if inconsist_nf:
        amostra = (
            nf[nf["REGISTRO_OPERADORA"].isin(inconsist_nf)]
            .groupby("REGISTRO_OPERADORA")["Nome_Fantasia"]
            .apply(lambda s: sorted(set(v for v in s if v and v.lower() != "nan"))[:5])
        )
        problemas.append(("Nome_Fantasia", amostra))

    # Modalidade
    md = (
        df[["REGISTRO_OPERADORA", "Modalidade"]]
        .dropna(subset=["REGISTRO_OPERADORA"])
        .copy()
    )
    md["Modalidade"] = md["Modalidade"].astype(str).str.strip()

    g_md = md.groupby("REGISTRO_OPERADORA")["Modalidade"].nunique(dropna=True)
    inconsist_md = g_md[g_md > 1].index.tolist()

    if inconsist_md:
        amostra = (
            md[md["REGISTRO_OPERADORA"].isin(inconsist_md)]
            .groupby("REGISTRO_OPERADORA")["Modalidade"]
            .apply(lambda s: sorted(set(v for v in s if v and v.lower() != "nan"))[:5])
        )
        problemas.append(("Modalidade", amostra))

    if problemas:
        msg = ["❌ Inconsistência detectada por REGISTRO_OPERADORA:"]
        for campo, serie in problemas:
            msg.append(f"\nCampo: {campo}")
            # mostra até 20 operadoras na mensagem para não explodir o terminal
            for op, vals in serie.head(20).items():
                msg.append(f"  - {op}: {vals}")
            if len(serie) > 20:
                msg.append(f"  ... e mais {len(serie) - 20} operadoras")
        raise ValueError("\n".join(msg))


def build_quarter_sheet(df_q: pd.DataFrame) -> pd.DataFrame:
    """
    Para um trimestre, agrega por operadora:
    - Nome_Fantasia (primeiro valor)
    - Modalidade (primeiro valor)
    - Eventos e Indenizações Líquidas = soma(31,311,312,32)
    - 41 = soma(conta 41)
    - RES_OPERACIONAL = Eventos - 41
    """
    meta = (
        df_q.sort_values(["REGISTRO_OPERADORA"])
            .groupby("REGISTRO_OPERADORA", as_index=True)
            .agg(
                Nome_Fantasia=("Nome_Fantasia", "first"),
                Modalidade=("Modalidade", "first"),
            )
    )

    eventos = (
        df_q[df_q["CD_CONTA_CONTABIL"].isin(EVENTOS_CONTAS)]
          .groupby("REGISTRO_OPERADORA")["Diferenca"]
          .sum()
          .rename("Eventos e Indenizações Líquidas")
    )

    v41 = (
        df_q[df_q["CD_CONTA_CONTABIL"].eq(CONTA_41)]
          .groupby("REGISTRO_OPERADORA")["Diferenca"]
          .sum()
          .rename("41")
    )

    out = meta.join(eventos, how="left").join(v41, how="left")
    out["Eventos e Indenizações Líquidas"] = out["Eventos e Indenizações Líquidas"].fillna(0)
    out["41"] = out["41"].fillna(0)
    out["RES_OPERACIONAL"] = out["Eventos e Indenizações Líquidas"] - out["41"]

    out = out.reset_index()
    out = out[
        [
            "REGISTRO_OPERADORA",
            "Nome_Fantasia",
            "Modalidade",
            "Eventos e Indenizações Líquidas",
            "41",
            "RES_OPERACIONAL",
        ]
    ].sort_values(["REGISTRO_OPERADORA"])

    return out


def main(input_file: str, output_file: str):
    input_path = Path(input_file)
    output_path = Path(output_file)

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

    # Normalizações de tipo
    df["REGISTRO_OPERADORA"] = df["REGISTRO_OPERADORA"]
    df["Trimestre"] = df["Trimestre"].astype(str).str.strip().str.upper()

    df["CD_CONTA_CONTABIL"] = pd.to_numeric(df["CD_CONTA_CONTABIL"], errors="coerce").astype("Int64")
    df["Diferenca"] = pd.to_numeric(df["Diferenca"], errors="coerce").fillna(0)

    # ✅ Validação global (arquivo inteiro)
    validar_consistencia(df)

    # Trimestres únicos ordenados
    trimestres = sorted(df["Trimestre"].dropna().unique(), key=trimestre_sort_key)

    # Escreve NOVO arquivo com abas por trimestre + Resumo
    # O ExcelWriter em modo "w" cria/reescreve o arquivo de saída. [2](https://medium.com/@udtc.us/understanding-the-cost-of-github-codespaces-a-deep-dive-into-2-core-instances-913a110eefb3)[1](https://bing.com/search?q=GitHub+Codespaces+prebuild+billing)
    with pd.ExcelWriter(output_path, engine="openpyxl", mode="w") as writer:
        # Aba Resumo (empilhada)
        resumo_parts = []

        for tri in trimestres:
            df_q = df[df["Trimestre"].eq(tri)].copy()
            sheet_df = build_quarter_sheet(df_q)
            sheet_df.insert(0, "Trimestre", tri)  # adiciona coluna trimestre no resumo

            # guarda para o Resumo
            resumo_parts.append(sheet_df)

            # escreve aba do trimestre
            sheet_name = sanitize_sheet_name(tri)
            sheet_df.drop(columns=["Trimestre"]).to_excel(writer, sheet_name=sheet_name, index=False)

        # escreve a aba Resumo
        if resumo_parts:
            resumo = pd.concat(resumo_parts, ignore_index=True)
            # ordenação: trimestre cronológico e operadora
            resumo["__sort__"] = resumo["Trimestre"].map(lambda x: trimestre_sort_key(x)[0] * 10 + trimestre_sort_key(x)[1])
            resumo = resumo.sort_values(["__sort__", "REGISTRO_OPERADORA"]).drop(columns="__sort__")
            resumo.to_excel(writer, sheet_name="Resumo", index=False)

    print(f"✅ Novo arquivo gerado: {output_path.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gera NOVO Excel com uma aba por trimestre (1T2023) + aba Resumo; valida consistência de Nome_Fantasia e Modalidade."
    )
    parser.add_argument("--input", required=True, help="Excel de entrada (saída do 1º script).")
