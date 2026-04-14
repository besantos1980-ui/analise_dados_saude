import pandas as pd
import requests
import zipfile
from io import BytesIO
from datetime import datetime

# Função para baixar e processar o arquivo de cadastro
def processar_cadastro():
    url = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        df = pd.read_csv(BytesIO(response.content), sep=';', encoding='latin1', low_memory=False)
        print(f"Colunas no arquivo de cadastro: {list(df.columns)}")
        # Renomear colunas
        df.rename(columns=lambda x: 'REGISTRO_OPERADORA' if 'REGISTRO' in x.upper() else x, inplace=True)
        df.rename(columns=lambda x: 'Nome_Fantasia' if 'NOME_FANTASIA' in x.upper() else x, inplace=True)
        df.rename(columns=lambda x: 'Modalidade' if 'MODALIDADE' in x.upper() else x, inplace=True)
        return df[['REGISTRO_OPERADORA', 'Nome_Fantasia', 'Modalidade']]
    except Exception as e:
        print(f"Erro ao processar cadastro: {e}")
        return pd.DataFrame()

# Função para processar dados contábeis
def processar_contabeis():
    anos = [2023, 2024, 2025]
    trimestres = [1, 2, 3, 4]
    resultados = []
    for ano in anos:
        for t in trimestres:
            url = f"https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/{ano}/{t}T{ano}.zip"
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()
                with zipfile.ZipFile(BytesIO(response.content)) as zf:
                    csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                    if not csv_files:
                        print(f"Nenhum CSV encontrado em {url}")
                        continue
                    with zf.open(csv_files[0]) as f:
                        chunks = pd.read_csv(f, chunksize=10000, sep=';', encoding='latin1', low_memory=False)
                        for chunk in chunks:
                            print(f"Processando chunk para {t}T{ano}, colunas: {list(chunk.columns)}")
                            # Renomear colunas
                            chunk.rename(columns=lambda x: 'REG_ANS' if 'REG_ANS' in x.upper() else x, inplace=True)
                            chunk.rename(columns=lambda x: 'CD_CONTA_CONTABIL' if 'CD_CONTA_CONTABIL' in x.upper() else x, inplace=True)
                            chunk.rename(columns=lambda x: 'VL_SALDO_INICIAL' if 'VL_SALDO_INICIAL' in x.upper() else x, inplace=True)
                            chunk.rename(columns=lambda x: 'VL_SALDO_FINAL' if 'VL_SALDO_FINAL' in x.upper() else x, inplace=True)
                            # Novo filtro
                            chunk = chunk[chunk['CD_CONTA_CONTABIL'].isin(['311', '3117', '3119', '41'])]
                            if chunk.empty:
                                continue
                            # Calcular diferença
                            chunk['VL_SALDO_INICIAL'] = pd.to_numeric(chunk['VL_SALDO_INICIAL'], errors='coerce')
                            chunk['VL_SALDO_FINAL'] = pd.to_numeric(chunk['VL_SALDO_FINAL'], errors='coerce')
                            chunk['Diferenca'] = chunk['VL_SALDO_FINAL'] - chunk['VL_SALDO_INICIAL']
                            chunk['Trimestre'] = f"{t}T{ano}"
                            resultados.append(chunk[['REG_ANS', 'Trimestre', 'CD_CONTA_CONTABIL', 'Diferenca']])
                print(f"Processado {t}T{ano}")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    print(f"Arquivo não existe: {url}")
                else:
                    print(f"Erro HTTP: {e}")
            except Exception as e:
                print(f"Erro ao processar {t}T{ano}: {e}")
    return pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame()

# Executar
cadastro = processar_cadastro()
contabeis = processar_contabeis()

if not cadastro.empty and not contabeis.empty:
    # Merge
    df_final = pd.merge(contabeis, cadastro, left_on='REG_ANS', right_on='REGISTRO_OPERADORA', how='inner')
    df_final = df_final[['REGISTRO_OPERADORA', 'Nome_Fantasia', 'Modalidade', 'Trimestre', 'CD_CONTA_CONTABIL', 'Diferenca']]
    # Salvar Excel
    filename = f"arquivo_base_cenario_saude_{datetime.today().strftime('%d_%m_%Y')}.xlsx"
    df_final.to_excel(filename, index=False)
    print(f"Arquivo salvo: {filename}")
else:
    print("Erro: Dados insuficientes para gerar o arquivo.")