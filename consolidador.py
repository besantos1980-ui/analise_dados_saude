import pandas as pd
import glob
import os
import re
from datetime import datetime

def extrair_data_arquivo(nome_arquivo):
    """Extrai a data do nome do arquivo (DD_MM_AAAA) e retorna um objeto datetime."""
    match = re.search(r'(\d{2}_\d{2}_\d{4})', nome_arquivo)
    if match:
        return datetime.strptime(match.group(1), '%d_%m_%Y')
    return datetime.min

def buscar_arquivo_mais_recente(padrao):
    """Busca na pasta o arquivo que segue o padrão e tem a data mais recente."""
    arquivos = glob.glob(f"{padrao}_*.xlsx")
    if not arquivos:
        print(f"Atenção: Nenhum arquivo encontrado para o padrão '{padrao}'")
        return None
    # Ordena pela data extraída do nome do arquivo
    arquivo_mais_recente = max(arquivos, key=extrair_data_arquivo)
    print(f"Selecionado: {arquivo_mais_recente}")
    return arquivo_mais_recente

def carregar_e_preparar(nome_base):
    arquivo = buscar_arquivo_mais_recente(nome_base)
    if arquivo:
        df = pd.read_excel(arquivo)
        # Garante que REG_ANS seja numérico para o merge
        df['REG_ANS'] = pd.to_numeric(df['REG_ANS'], errors='coerce').astype('Int64')
        return df
    return pd.DataFrame()

def main():
    print("Iniciando a consolidação final dos dados...\n")

    # 1. Carregar todos os arquivos fontes mais recentes
    df1 = carregar_e_preparar("base_financeira_agrupada_dinamica")
    df2 = carregar_e_preparar("base_auxresbru_dinamica")
    df3 = carregar_e_preparar("base_calculo_dinamico_X")
    df4 = carregar_e_preparar("base_fin_patrimonial_dinamica")
    df5 = carregar_e_preparar("base_impostos_participacoes")

    if df1.empty:
        print("Erro: O arquivo base financeiro é essencial e não foi encontrado.")
        return

    # 2. Unir os arquivos (Merge Sequencial)
    # Usamos outer join para não perder dados se uma operadora faltar em algum arquivo
    print("\nCruzando informações de todas as fontes...")
    df_master = df1.merge(df2, on=['REG_ANS', 'Trimestre'], how='outer')
    df_master = df_master.merge(df3, on=['REG_ANS', 'Trimestre'], how='outer')
    df_master = df_master.merge(df4, on=['REG_ANS', 'Trimestre'], how='outer', suffixes=('', '_PATRIMONIAL'))
    df_master = df_master.merge(df5, on=['REG_ANS', 'Trimestre'], how='outer', suffixes=('', '_IMPOSTOS'))

    # Preencher valores nulos com 0 para cálculos
    df_master = df_master.fillna(0)

    # 3. Processar cálculos das colunas finais
    print("Aplicando fórmulas contábeis finais...")
    
    # Coluna 10: Resultado Bruto (Resultado Op. Planos + AuxResBru)
    df_master['Resultado Bruto'] = df_master['Resultado'] + df_master['AuxResBru']
    
    # Coluna 11: Resultado Operacional (Resultado Bruto - X)
    df_master['Resultado Operacional'] = df_master['Resultado Bruto'] - df_master['X']
    
    # Coluna 14: Resultado Antes impostos (Resultado Operacional - Resultado Final Patrimonial)
    # Nota: Usamos o sufixo _PATRIMONIAL definido no merge caso o nome colida
    df_master['Resultado Antes impostos'] = df_master['Resultado Operacional'] - df_master['Resultado Final']
    
    # Coluna 19: Resultado Líquido (Resultado Antes impostos - Resultado Final Impostos)
    df_master['Resultado Líquido'] = df_master['Resultado Antes impostos'] - df_master['Resultado Final_IMPOSTOS']

    # 4. Organizar e renomear colunas para o formato final
    df_final = df_master[[
        'REG_ANS', 'Trimestre', 
        'Contraprestações efetivas',       # Col 1
        'Eventos Líquidos',                 # Col 2
        'Resultado',                        # Col 3 (Resultado Operações Planos)
        '331',                              # Col 4
        '332',                              # Col 5
        '333',                              # Col 6
        '34',                               # Col 7
        '441',                              # Col 8
        '442',                              # Col 9
        'Resultado Bruto',                  # Col 10
        'Resultado Operacional',            # Col 11
        'Resultado Financeiro Líquido',     # Col 12
        'Resultado Patrimonial',            # Col 13
        'Resultado Antes impostos',         # Col 14
        '6111',                             # Col 15
        '6112',                             # Col 16
        '6119',                             # Col 17
        '612',                              # Col 18
        'Resultado Líquido'                 # Col 19
    ]].copy()

    # 5. Gerar arquivo Excel com abas por Trimestre
    ts = datetime.today().strftime("%d_%m_%Y")
    nome_saida = f"CONSOLIDADO_FINAL_ANS_{ts}.xlsx"
    
    print(f"\nGerando arquivo final: {nome_saida}")
    
    with pd.ExcelWriter(nome_saida, engine='openpyxl') as writer:
        # Ordenar trimestres (mais recentes primeiro)
        trimestres = sorted(df_final['Trimestre'].unique(), reverse=True)
        
        for tri in trimestres:
            df_tri = df_final[df_final['Trimestre'] == tri].drop(columns=['Trimestre'])
            # Ordenar por REG_ANS para facilitar a leitura
            df_tri = df_tri.sort_values('REG_ANS')
            df_tri.to_excel(writer, sheet_name=str(tri), index=False)

    print(f"\n=== SUCESSO ===")
    print(f"O arquivo {nome_saida} foi gerado com {len(trimestres)} abas.")
    print("Processamento de toda a esteira ANS concluído.")

if __name__ == "__main__":
    main()
