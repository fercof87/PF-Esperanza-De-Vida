import streamlit as st
import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

#logo
st.sidebar.image('Fixing Data.jpg', caption='Proyecto Final')
#---------------------------------------------------------------
#titulo
st.title('Estudio por región utilizando Machine Learning')

st.markdown('**Explorando la Viabilidad de Comercializar un Multivitaminico para Personas Mayores de 65 años**')


#-------------------------------------------------------------------------
# Modelo
df_escaler = pd.read_csv('data_ML.csv')
num_clusters = 5

kmeans = KMeans(n_clusters=num_clusters, random_state=42)

kmeans.fit(df_escaler)

cluster_labels = kmeans.labels_

df_cluster = df_escaler.assign(Cluster=cluster_labels)

#------------------------------------------------------------------------------
# Union con la base original para clusterizar los países
df_data = pd.read_csv('data.csv')

df_group_cluster = df_data.assign(Cluster=cluster_labels)

#Después de analizar los clusters, se observó que el cluster más óptimo es el número 1.
df_cluster_1 = df_group_cluster[df_group_cluster["Cluster"]==1]


#-----------------------------------------------------------------------------
# Mostrar información del cluster
# Primer filtro por continente
opciones_continentes = ['Selecciona un Continente'] + list(df_cluster_1['Continente'].unique())
continente_seleccionado = st.selectbox("Selecciona un Continente", opciones_continentes)

# Verificar si se ha seleccionado un continente
if continente_seleccionado != 'Selecciona un Continente':
    # Filtrar por continente y seleccionar la columna 'Nombre_Pais'
    df_filtrado = df_cluster_1[df_cluster_1['Continente'] == continente_seleccionado]['Nombre_Pais'].tolist()

    # Mostrar los países del continente
    texto_paises = '<br>'.join(df_filtrado)
    st.markdown(f"**<span style='font-size:22px'>{texto_paises}</span>**", unsafe_allow_html=True)

# Segundo filtro por país
opciones_paises = ['Selecciona un País'] + list(df_group_cluster['Nombre_Pais'].unique())
pais_seleccionado = st.selectbox("Selecciona un País", opciones_paises)

# Verificar si se ha seleccionado un país
if pais_seleccionado != 'Selecciona un País':
    # Verificar la viabilidad del país seleccionado
    if pais_seleccionado in df_cluster_1['Nombre_Pais'].values:
        st.markdown(f"<span style='font-size:24px'>El país **{pais_seleccionado}** es viable.</span>", unsafe_allow_html=True)
    else:
        st.markdown(f"<span style='font-size:18px'>El país **{pais_seleccionado}** No es viable.</span>", unsafe_allow_html=True)