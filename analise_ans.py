import requests
from io import BytesIO
import pandas as pd
import zipfile
from datetime import datetime
import os

# Função para baixar e processar o cadastro das operadoras
def processar_cadastro():
    url = 'https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv'
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Levanta erro se não for 200
        data = BytesIO(response.content)
        df = pd.read_csv(data, sep=';', encoding='latin1', low_memory=False)
        print('Colunas do cadastro:', df.columns.tolist())
        # Renomear colunas de forma case-insensitive
        rename_dict = {}
        for col in df.columns:
            if col.upper().startswith('REGISTRO'):
                rename_dict[col] = 'REGISTRO_OPERADORA'
            elif col.upper().startswith('NOME_FANTASIA'):
                rename_dict[col] = 'Nome_Fantasia'
            elif col.upper().startswith('MODALIDADE'):
                rename_dict[col] = 'Modalidade'
        df.rename(columns=rename_dict, inplace=True)
        df.to_csv('cadop_debug.csv', index=False)
        print('Cadastro: {} linhas'.format(len(df)))
        print('Amostras do cadastro:')
        print(df.head(3))
        return df
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            print('Arquivo de cadastro não encontrado (404). Pulando...')
            return pd.DataFrame()
        else:
            print('Erro ao baixar cadastro:', e)
            return pd.DataFrame()
    except Exception as e:
        print('Erro geral no cadastro:', e)
        return pd.DataFrame()

# Função para processar os dados contábeis
def processar_contabeis():
    anos = [2023, 2024, 2025]
    all_chunks = []
    for ano in anos:
        for t in range(1, 5):
            url = f'https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/{ano}/{t}T{ano}.zip'
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()
                data = BytesIO(response.content)
                with zipfile.ZipFile(data) as zf:
                    csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                    if not csv_files:
                        print(f'Nenhum CSV encontrado no ZIP para {t}T{ano}. Pulando...')
                        continue
                    with zf.open(csv_files[0]) as file:
                        for chunk in pd.read_csv(file, sep=';', encoding='latin1', chunksize=10000):
                            print(f'Valores únicos CD_CONTA_CONTABIL em {t}T{ano}: {chunk["CD_CONTA_CONTABIL"].dropna().unique()[:10].tolist()}')  # Top 10 para ver formato
                            required_cols = ['REG_ANS', 'CD_CONTA_CONTABIL', 'VL_SALDO_INICIAL', 'VL_SALDO_FINAL']
                            if not all(col in chunk.columns for col in required_cols):
                             print(f'{t}T{ano}: Pulando chunk sem colunas requeridas ({set(required_cols) - set(chunk.columns)})')
                             continue
                            chunk = chunk[chunk['CD_CONTA_CONTABIL'].isin([311, 3117, 3119, 41])]#numéricos, não strings
                            print(f'{t}T{ano}: {len(chunk)} linhas pós-filtro')
                            # Renomear colunas
                            rename_dict = {}
                            for col in chunk.columns:
                                if col.upper().startswith('REG_ANS'):
                                    rename_dict[col] = 'REG_ANS'
                                elif col.upper().startswith('CD_CONTA_CONTABIL'):
                                    rename_dict[col] = 'CD_CONTA_CONTABIL'
                                elif col.upper().startswith('VL_SALDO_INICIAL'):
                                    rename_dict[col] = 'VL_SALDO_INICIAL'
                                elif col.upper().startswith('VL_SALDO_FINAL'):
                                    rename_dict[col] = 'VL_SALDO_FINAL'
                            chunk.rename(columns=rename_dict, inplace=True)
                            # Filtro
                                                      
                            if 'VL_SALDO_INICIAL' not in chunk.columns or 'VL_SALDO_FINAL' not in chunk.columns:
                                print(f'{t}T{ano}: Pulando chunk sem saldos')
                                continue
                            chunk['VL_SALDO_INICIAL'] = pd.to_numeric(chunk['VL_SALDO_INICIAL'].astype(str).str.replace('.', '').str.replace(',', '.'), errors='coerce')
                            chunk['VL_SALDO_FINAL'] = pd.to_numeric(chunk['VL_SALDO_FINAL'].astype(str).str.replace('.', '').str.replace(',', '.'), errors='coerce')
                            # Diferença
                            chunk['Diferenca'] = chunk['VL_SALDO_FINAL'] - chunk['VL_SALDO_INICIAL']
                            # Trimestre
                            chunk['Trimestre'] = f'{t}T{ano}'
                            print(f'{t}T{ano}: {len(chunk)} linhas prontas para concat (colunas: {chunk.columns.tolist()[:5]}...)')
                            all_chunks.append(chunk)
            except requests.exceptions.HTTPError as e:
                if response.status_code == 404:
                    print(f'Arquivo contábil {t}T{ano} não encontrado (404). Pulando...')
                    continue
                else:
                    print(f'Erro ao baixar contábil {t}T{ano}:', e)
                    continue
            except Exception as e:
                print(f'Erro geral no contábil {t}T{ano}:', e)
                continue
    if all_chunks:
        df_contabeis = pd.concat(all_chunks, ignore_index=True)
        df_contabeis.to_csv('contabeis_debug.csv', index=False)
        print('Contábeis: {} linhas'.format(len(df_contabeis)))
        print('Amostras dos contábeis:')
        print(df_contabeis.head(3))
        return df_contabeis
    else:
        print('Nenhum dado contábil processado.')
        return pd.DataFrame()

# Função principal
def main():
    df_cadastro = processar_cadastro()
    df_contabeis = processar_contabeis()
    if not df_cadastro.empty and not df_contabeis.empty:
        # Merge inner
        df_merged = pd.merge(df_contabeis, df_cadastro, left_on='REG_ANS', right_on='REGISTRO_OPERADORA', how='inner')
        # Selecionar colunas
        df_final = df_merged[['REGISTRO_OPERADORA', 'Nome_Fantasia', 'Modalidade', 'Trimestre', 'CD_CONTA_CONTABIL', 'Diferenca']]
        print('Merge: {} linhas'.format(len(df_final)))
        if len(df_final) > 0:
            filename = f'arquivo_base_cenario_saude_{datetime.today().strftime("%d_%m_%Y")}.xlsx'
            df_final.to_excel(filename, index=False)
            print(f'Arquivo Excel salvo: {filename}')
        else:
            print('Nenhuma linha para salvar no Excel.')
    else:
        print('Dados insuficientes para merge.')

if __name__ == '__main__':
    main()