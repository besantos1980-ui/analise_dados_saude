import requests
from io import BytesIO
import pandas as pd
import zipfile
from datetime import datetime

# Função para processar cadastro (CADOP)
def processar_cadastro():
    url = 'https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv'
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        df = pd.read_csv(BytesIO(response.content), sep=';', encoding='latin1', low_memory=False)
        print('Colunas cadastro:', df.columns.tolist())
        
        # Renomear case-insens
        rename_dict = {}
        for col in df.columns:
            col_upper = col.upper()
            if col_upper.startswith('REGISTRO'):
                rename_dict[col] = 'REGISTRO_OPERADORA'
            elif col_upper.startswith('NOME_FANTASIA'):
                rename_dict[col] = 'Nome_Fantasia'
            elif col_upper.startswith('MODALIDADE'):
                rename_dict[col] = 'Modalidade'
        
        df.rename(columns=rename_dict, inplace=True)
        df.to_csv('cadop_debug.csv', index=False)
        print(f'Cadastro: {len(df)} linhas')
        return df
    except Exception as e:
        print('Erro cadastro:', e)
        return pd.DataFrame()

# Função para processar os dados contábeis
def processar_contabeis():
    anos = [2023, 2024, 2025]
    all_chunks = []
    
    # Transformando as contas de interesse em strings para evitar falha no filtro
    contas_alvo = ['31', '311', '312', '32', '41']
    
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
                        # Adicionado decimal=',' para tratar os valores financeiros da ANS
                        for chunk in pd.read_csv(file, sep=';', encoding='latin1', chunksize=10000, decimal=','):
                            
                            # Padroniza nomes das colunas
                            rename_dict = {}
                            for col in chunk.columns:
                                col_upper = col.upper()
                                if col_upper.startswith('REG_ANS'):
                                    rename_dict[col] = 'REG_ANS'
                                elif col_upper.startswith('CD_CONTA_CONTABIL'):
                                    rename_dict[col] = 'CD_CONTA_CONTABIL'
                                elif col_upper.startswith('VL_SALDO_INICIAL'):
                                    rename_dict[col] = 'VL_SALDO_INICIAL'
                                elif col_upper.startswith('VL_SALDO_FINAL'):
                                    rename_dict[col] = 'VL_SALDO_FINAL'
                            chunk.rename(columns=rename_dict, inplace=True)
                            
                            required_cols = ['REG_ANS', 'CD_CONTA_CONTABIL', 'VL_SALDO_INICIAL', 'VL_SALDO_FINAL']
                            if not all(col in chunk.columns for col in required_cols):
                                print(f'{t}T{ano}: Pulando chunk sem colunas requeridas')
                                continue
                            
                            # Garante que a coluna de conta é string antes do filtro
                            chunk['CD_CONTA_CONTABIL'] = chunk['CD_CONTA_CONTABIL'].astype(str)
                            chunk = chunk[chunk['CD_CONTA_CONTABIL'].isin(contas_alvo)]
                            
                            # Erro de indentação corrigido aqui:
                            if 'VL_SALDO_INICIAL' not in chunk.columns or 'VL_SALDO_FINAL' not in chunk.columns:
                                print(f'{t}T{ano}: Pulando chunk sem saldos')
                                continue
                            
                            chunk['VL_SALDO_INICIAL'] = pd.to_numeric(chunk['VL_SALDO_INICIAL'], errors='coerce')
                            chunk['VL_SALDO_FINAL'] = pd.to_numeric(chunk['VL_SALDO_FINAL'], errors='coerce')
                            
                            # Dropa linhas que viraram NaN na conversão
                            chunk.dropna(subset=['VL_SALDO_INICIAL', 'VL_SALDO_FINAL'], inplace=True)
                            
                            chunk['Diferenca'] = chunk['VL_SALDO_FINAL'] - chunk['VL_SALDO_INICIAL']
                            chunk['Trimestre'] = f'{t}T{ano}'
                            
                            all_chunks.append(chunk)
                            
            except requests.exceptions.HTTPError as e:
                if response.status_code == 404:
                    print(f'Arquivo contábil {t}T{ano} não encontrado (404). Pulando...')
                else:
                    print(f'Erro ao baixar contábil {t}T{ano}:', e)
            except Exception as e:
                print(f'Erro geral no contábil {t}T{ano}:', e)
                
    if all_chunks:
        df_contabeis = pd.concat(all_chunks, ignore_index=True)
        return df_contabeis
    else:
        print('Nenhum dado contábil processado.')
        return pd.DataFrame()

# Função principal
def main():
    df_cadastro = processar_cadastro()
    df_contabeis = processar_contabeis()
    
    if not df_cadastro.empty and not df_contabeis.empty:
        df_merged = pd.merge(df_contabeis, df_cadastro, left_on='REG_ANS', right_on='REGISTRO_OPERADORA', how='inner')
        df_final = df_merged[['REGISTRO_OPERADORA', 'Nome_Fantasia', 'Modalidade', 'Trimestre', 'CD_CONTA_CONTABIL', 'Diferenca']]
        
        print(f'Merge: {len(df_final)} linhas')
        if len(df_final) > 0:
            filename = f'arquivo_base_cenario_saude_{datetime.today().strftime("%d_%m_%Y")}.xlsx'
            # Necessita 'pip install openpyxl'
            df_final.to_excel(filename, index=False)
            print(f'Arquivo Excel salvo: {filename}')
        else:
            print('Nenhuma linha para salvar no Excel.')
    else:
        print('Dados insuficientes para merge.')

if __name__ == '__main__':
    main()
