  import argparse
import re
from pathlib import Path

import pandas as pd

EVENTOS_CONTAS = {311, 3117, 3119}
CONTA_41 = 41


def sanitize_sheet_name(name: str) -> str:
    """Garante nome de aba válido no Excel (máx. 31 chars e sem caracteres proibidos)."""
    name = str(name)
    name = re.sub(r"[:\\/?*\[\]]", "_", name)
    return name[:31]


def trimestre_sort_key(tri: str):
    """
    Ordena trimestres no formato XTAA (ex.: 1T23) de forma cronológica.
    Retorna (ano, trimestre). Se não casar com o padrão, joga pro final.
    """
    tri = str(tri).strip().upper()
    m = re.match(r"^([1-4])T(\d{2})$", tri)
    if not m:
        return (9999, 9)
    q = int(m.group(1))
    yy = int(m.group(2))
    # Ajuste simples: 00-79 => 2000-2079; 80-99 => 1980-1999 (se precisar)
    year = 2000 + yy if yy <= 79 else 1900 + yy
    return (year, q)


def build_quarter_sheet(df_q: pd.DataFrame) -> pd.DataFrame:
    # Metadados por operadora (pega o primeiro valor; assume consistência)
    meta = (
        df_q.sort_values(["REGISTRO_OPERADORA"])
            .groupby("REGISTRO_OPERADORA", as_index=True)
            .agg(
                Nome_Fantasia=("Nome_Fantasia", "first"),
                Modalidade=("Modalidade", "first"),
            )
    )

    # Eventos e Indenizações Líquidas = soma das contas 311 + 3117 + 3119
    eventos = (
        df_q[df_q["CD_CONTA_CONTABIL"].isin(EVENTOS_CONTAS)]
          .groupby("REGISTRO_OPERADORA")["Diferenca"]
          .sum()
          .rename("Eventos e Indenizações Líquidas")
    )

    # Coluna "41" = soma da conta 41
    v41 = (
        df_q[df_q["CD_CONTA_CONTABIL"].eq(CONTA_41)]
          .groupby("REGISTRO_OPERADORA")["Diferenca"]
          .sum()
          .rename("41")
    )

    # Junta tudo
    out = meta.join(eventos, how="left").join(v41, how="left")

    # Preenche ausências com zero
    out["Eventos e Indenizações Líquidas"] = out["Eventos e Indenizações Líquidas"].fillna(0)
    out["41"] = out["41"].fillna(0)

    # RES_OPERACIONAL = Eventos e Indenizações Líquidas - 41
    out["RES_OPERACIONAL"] = out["Eventos e Indenizações Líquidas"] - out["41"]

    # Organiza colunas finais
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

    # Lê o Excel de entrada (arquivo do 1º script)
    df = pd.read_excel(input_path, engine="openpyxl")

    # Valida colunas esperadas
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

    # Converte tipos para evitar inconsistências
    df["CD_CONTA_CONTABIL"] = pd.to_numeric(df["CD_CONTA_CONTABIL"], errors="coerce").astype("Int64")
    df["Diferenca"] = pd.to_numeric(df["Diferenca"], errors="coerce").fillna(0)
    df["Trimestre"] = df["Trimestre"].astype(str).str.strip()

    # Lista de trimestres únicos (ordenados cronologicamente)
    trimestres = sorted(df["Trimestre"].dropna().unique(), key=trimestre_sort_key)

    # Cria NOVO arquivo com uma aba por trimestre
    # Para escrever múltiplas abas no mesmo arquivo, use ExcelWriter + to_excel(sheet_name=...). [1](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_excel.html)[2](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.ExcelWriter.html)
    with pd.ExcelWriter(output_path, engine="openpyxl", mode="w") as writer:
        for tri in trimestres:
            df_q = df[df["Trimestre"].eq(tri)].copy()
            sheet_df = build_quarter_sheet(df_q)
            sheet_name = sanitize_sheet_name(tri)
            sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"✅ Novo arquivo gerado: {output_path.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Lê o Excel do analise_ans.py e gera NOVO Excel com abas por trimestre."
    )
    parser.add_argument("--input", required=True, help="Excel de entrada (saída do 1º script).")
    parser.add_argument("--output", required=True, help="Excel de saída (novo arquivo com abas).")
    args = parser.parse_args()

    main(args.input, args.output)
