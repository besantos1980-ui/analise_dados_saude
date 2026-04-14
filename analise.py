import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_excel('dados.xlsx')
df = df.dropna(subset=['CPF'])
resumo = df.groupby('Plano')['Reembolso'].sum()
resumo.plot(kind='bar')
plt.title('Reembolsos por Plano')
plt.savefig('grafico.png')
resumo.to_excel('output.xlsx')
print('Análise concluída! Veja grafico.png e output.xlsx')
