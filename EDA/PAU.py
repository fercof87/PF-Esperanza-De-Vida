import pandas as pd

df_final = pd.read_csv("banco_mundial_data_indicator_to_row.csv")
df_indicadores = pd.read_csv("indicadores.csv")

mi_diccionario = df_indicadores.set_index('cod_indicator')['name_indicator'].to_dict()
df_final['Indicador_name'] = df_final['Indicador'].map(mi_diccionario)

df_agg = df_final.groupby('Indicador')['Valor'].count().to_frame().sort_values('Valor', ascending=False)
df_agg_filtrado = df_agg[df_agg['Valor']> 1000].reset_index()
indicadores = df_agg_filtrado['Indicador']

df_final = df_final[df_final['Indicador'].isin(indicadores)]

paises = df_final['Pais'].unique().tolist()
indicadores = df_final['Indicador_name'].unique().tolist()
anios = df_final['Año'].unique().tolist()

df_final.loc[(df_final['Pais'] == "SGP") & (df_final['Indicador_name'] == "Rural population growth (annual %)"), 'Valor'] = 0

for pais in paises:
    for indicador in indicadores:
        df_pais_indicador = df_final[(df_final['Pais']==pais) & (df_final['Indicador_name']==indicador)]
        nulos = df_pais_indicador['Valor'].isna().sum()
        if(nulos > 0.4*(len(anios))):
            indices = df_pais_indicador.index
            df_final.drop(index=indices, inplace=True)
        else:
            df_final.loc[(df_final['Pais']==pais) & (df_final['Indicador_name']==indicador) & (df_final['Valor'].isna()), 'Valor'] = df_pais_indicador['Valor'].mean()

df_final.to_csv("nueocv.csv", index=False)
