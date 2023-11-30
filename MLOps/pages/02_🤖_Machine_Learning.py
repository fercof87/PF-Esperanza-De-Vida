import streamlit as st
import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns


st.sidebar.write("# Explorando la Viabilidad de Comercializar un Multivitaminico para Personas Mayores de 65 años")
#logo
st.sidebar.image('../MLOps/img/Fixing_Data.jpg', caption='Proyecto Final')

#---------------------------------------------------------------
#titulo
st.title('Estudio por región utilizando Machine Learning')

st.markdown('**Explorando la Viabilidad de Comercializar un Multivitaminico para Personas Mayores de 65 años**')


#-------------------------------------------------------------------------
# Modelo
df_escaler = pd.read_csv('../MLOps/datasets_ML/data_ML.csv')
num_clusters = 5

kmeans = KMeans(n_clusters=num_clusters, random_state=42)

kmeans.fit(df_escaler)

cluster_labels = kmeans.labels_

df_cluster = df_escaler.assign(Cluster=cluster_labels)

#------------------------------------------------------------------------------
# Union con la base original para clusterizar los países
df_data = pd.read_csv('../MLOps/datasets_ML/data.csv')
df_data_año = pd.read_csv('../MLOps/datasets_ML/data_año_cluster.csv')
df_group_cluster = df_data.assign(Cluster=cluster_labels)

#Después de analizar los clusters, se observó que el cluster más óptimo es el número 2.
df_cluster_2 = df_group_cluster[df_group_cluster["Cluster"]==2]
df_data_año['Año'] = df_data_año['Año'].astype(str)

#-----------------------------------------------------------------------------
# Mostrar información del cluster
# Primer filtro por continente
opciones_continentes = ['Selecciona un Continente'] + list(df_cluster_2['Continente'].unique())
continente_seleccionado = st.selectbox("Selecciona un Continente", opciones_continentes)

# Verificar si se ha seleccionado un continente
if continente_seleccionado != 'Selecciona un Continente':
    #Filtrar por continente y seleccionar la columna 'Nombre_Pais'
    df_filtrado = df_cluster_2[df_cluster_2['Continente'] == continente_seleccionado]['Pais'].tolist()

    # Mostrar los países del continente
    texto_paises = '<br>'.join(df_filtrado)
    st.markdown(f"**<span style='font-size:22px'>{texto_paises}</span>**", unsafe_allow_html=True)

# Segundo filtro por país
opciones_paises = ['Selecciona un País'] + list(df_group_cluster['Pais'].unique())
pais_seleccionado = st.selectbox("Selecciona un País", opciones_paises)

if pais_seleccionado != 'Selecciona un País':
    if pais_seleccionado in df_cluster_2['Pais'].values:
        st.markdown(f"<span style='font-size:24px'>El país **{pais_seleccionado}** es viable.</span>", unsafe_allow_html=True)
        with st.expander("Ver Datos del País"):
            st.dataframe(df_data_año[df_data_año['Pais'] == pais_seleccionado])

        with st.expander(f"Visualización de Datos de {pais_seleccionado}"):
            df_gra_pais = df_data_año[df_data_año['Pais'] == pais_seleccionado]
            # Establecer el estilo y contexto de Seaborn
            sns.set(style="darkgrid", context="talk")
        # Primer gráfico: Línea de tiempo
            fig1, ax1 = plt.subplots(figsize=(10, 6))
            sns.lineplot(data=df_gra_pais, x='Año', y='GDP per capita (current US$)', linewidth=2, marker='o',color='orange')
            ax1.set_title("Linea de Tiempo del PBI per capita del País seleccionado")
            ax1.set_xlabel("Año")
            ax1.set_ylabel("US$ Actuales")
            # Aumentar el tamaño de la fuente de los ticks en los ejes
            ax1.tick_params(axis='both', which='major', labelsize=16)
            st.pyplot(fig1) 
            

            #Segundo gráfico: Otro tipo de gráfico (por ejemplo, un histograma)
            fig2, ax2 = plt.subplots(figsize=(10, 6))
            sns.lineplot(data=df_gra_pais,x='Año', y='Life expectancy at birth, total (years)', linewidth=2, marker='o',color='black')  # Reemplaza 'Otra_Columna' con el nombre real
            ax2.set_title("Esperanza de Vida en los últimos 5 Años del País seleccionado")
            ax2.set_xlabel("Año")
            ax2.set_ylabel("Esperanza de Vida en Años")
            # Aumentar el tamaño de la fuente de los ticks en los ejes
            ax2.tick_params(axis='both', which='major', labelsize=16)
            st.pyplot(fig2) 
    else:
        st.markdown(f"<span style='font-size:18px'>El país **{pais_seleccionado}** no es viable.</span>", unsafe_allow_html=True)
