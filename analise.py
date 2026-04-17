import pandas as pd
import matplotlib.pyplot as plt

try:
    # Carregar dados
    df = pd.read_excel('dados.xlsx')
    print('Colunas encontradas:', list(df.columns))
    print('Primeiras linhas:')
    print(df.head())
    
    # Limpeza
    if 'CPF' in df.columns:
        df = df.drop_duplicates(subset='CPF')
    df = df.dropna(subset=['CPF', 'Idade'] if 'CPF' in df.columns else ['Idade'])
    
    # Colunas numéricas
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    print(f'Colunas numéricas: {numeric_cols}')
    
    for col in numeric_cols:
        print(f'\nPara {col}:')
        print(f'Soma: {df[col].sum()}')
        print(f'Média: {df[col].mean()}')
        print(f'Top 5 maiores: {df[col].nlargest(5).tolist()}')
    
    # Agrupamento
    if 'Plano' in df.columns:
        group_col = 'Plano'
    else:
        df['Faixa_Idade'] = pd.cut(df['Idade'], bins=[0, 30, 50, 100], labels=['<30', '30-50', '>50'])
        group_col = 'Faixa_Idade'
    
    grouped = df.groupby(group_col)[numeric_cols[0] if numeric_cols else 'Idade'].sum()
    print(f'\nAgrupamento por {group_col}:')
    print(grouped)
    
    # Gráfico
    grouped.plot(kind='bar')
    plt.title('Soma por Grupo')
    plt.savefig('grafico.png')
    print('Gráfico salvo como grafico.png')
    
    # Exportar resumo
    summary = df.describe()
    summary.to_excel('output.xlsx')
    print('Resumo exportado para output.xlsx')
    
    print('\nAnálise concluída com sucesso!')
    
except FileNotFoundError:
    print('Erro: Arquivo dados.xlsx não encontrado.')
except Exception as e:
    print(f'Erro inesperado: {e}')