import streamlit as st
import pandas as pd
from sklearn.preprocessing import StandardScaler


#logo
st.sidebar.image('Fixing Data.jpg', caption='Proyecto Final')
#---------------------------------------------------------------
#titulo
st.title('data')
#Carga de datos
df = pd.read_csv('../EDA/Data_preparada/nueocv.csv')
#transformación del DF
df_pivot = pd.pivot_table(data=df, index=['Pais', "Año"], columns='Indicador_name', values='Valor')
df_pivot.reset_index(inplace=True)

#creacion de dos nuevas columnas

df_pivot["ratio_population ages 65 and above"] = df_pivot["Population ages 65 and above, total"]/df_pivot["Population, total"]
df_pivot["ratio_urban population"] = df_pivot["Urban population"]/df_pivot["Population, total"]
#seleccion de columnas
columnas_seleccionadas = ["Pais","Año","GDP per capita (current US$)","Inflation, GDP deflator (annual %)", "Inflation, consumer prices (annual %)","Life expectancy at birth, total (years)","Population growth (annual %)","ratio_population ages 65 and above","ratio_urban population", "Urban population growth (annual %)"]
df_pivot = df_pivot[columnas_seleccionadas]

df_pivot_años = df_pivot[df_pivot["Año"].isin([2018,2019,2020,2021,2022])]
df_pivot_años.reset_index(drop=True, inplace=True)
st.dataframe(df_pivot_años)

df_pivot_años.fillna(0, inplace=True)

df_pivot_años_group = df_pivot_años.groupby("Pais").mean().drop(columns=['Año'])
df_pivot_años_group.reset_index(inplace=True)


#-----------------------------------------------------------------------------------