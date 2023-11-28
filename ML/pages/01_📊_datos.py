import streamlit as st
import pandas as pd
from sklearn.preprocessing import StandardScaler
import seaborn as sns
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from google.oauth2 import service_account
from google.cloud import storage
import io
from sklearn.metrics import silhouette_score, silhouette_samples
import plotly.express as px


#logo
st.sidebar.image('Fixing Data.jpg', caption='Proyecto Final')
#---------------------------------------------------------------
#titulo
st.title('Proceso para el Modelo')


st.markdown("### Se hace la conexion a google storage para Obtener los datos")
st.image("google storage.png")

st.write('obtenemos los datos y los pasamos a un Dataframe')
#-------------------------------------------------------------------

# Cargar las credenciales desde el archivo JSON
credentials = service_account.Credentials.from_service_account_file('credenciales.json')

# Crear un cliente de Storage con las credenciales
client = storage.Client(credentials=credentials)

# Obtén el bucket y el blob
bucket_name = 'pf-henry-esperanza-mlops'
file_name = 'Data-ML.csv'
bucket = client.get_bucket(bucket_name)
blob = bucket.blob(file_name)

# Descargar el archivo a un DataFrame de Pandas
content = blob.download_as_bytes()
df = pd.read_csv(io.BytesIO(content),sep=';')
st.dataframe(df.head())

#----------------------------------------------------------------------------
#Transformamos el nombre de la columna anio a año
df.rename(columns={'Anio': 'Año'}, inplace=True)

#----------------------------------------------------------------------------

st.title('Transformamos el DF')
#transformación del DF
df_pivot = pd.pivot_table(data=df, index=['Pais', "Año", "Continente"], columns='Indicador', values='Valor')
df_pivot.reset_index(inplace=True)
st.dataframe(df_pivot)

st.title('Feature Engeneering')
st.write('Creación de dos nuevas columnas **["ratio_population ages 65 and above"]** y **["ratio_urban population"]**')
#creacion de dos nuevas columnas

df_pivot["ratio_population ages 65 and above"] = df_pivot["Population ages 65 and above, total"]/df_pivot["Population, total"]
df_pivot["ratio_urban population"] = df_pivot["Urban population"]/df_pivot["Population, total"]

#seleccion de columnas
columnas_seleccionadas = ["Pais","Año","Continente","GDP per capita (current US$)","Inflation, GDP deflator (annual %)", "Inflation, consumer prices (annual %)","Life expectancy at birth, total (years)","Population growth (annual %)","ratio_population ages 65 and above","ratio_urban population", "Urban population growth (annual %)"]
df_pivot = df_pivot[columnas_seleccionadas]

df_pivot_años = df_pivot[df_pivot["Año"].isin([2017,2018,2019,2020,2021])]
df_pivot_años.reset_index(drop=True, inplace=True)


df_pivot_años.fillna(0, inplace=True)

df_pivot_años_group = df_pivot_años.groupby(["Pais","Continente"]).mean().drop(columns=['Año'])
df_pivot_años_group.reset_index(inplace=True)
st.dataframe(df_pivot_años_group)
st.write('Se agrupa al Dataframe por **País y Continente** al mismo tiempo filtramos los ultimos 5 años **[2017,2018,2019,2020,2021]**')
#-----------------------------------------------------------------------------------
# Proceso para Standarizar datos
# Creación de una instancia de StandardScaler
scaler = StandardScaler()

# Selección de columnas para escalar desde un DataFrame (df_pivot_años_group)
# Ajustar el índice de las columnas (en este caso, [2:]) según la estructura de sus datos
columnas = df_pivot_años_group.columns[2:]

# Escalado de las columnas seleccionadas utilizando StandardScaler
df_pivot_años_group_escaler = scaler.fit_transform(df_pivot_años_group[columnas])

# Creación de un nuevo DataFrame con los valores escalados, manteniendo los nombres de las columnas
df_pivot_años_group_escaler = pd.DataFrame(df_pivot_años_group_escaler, columns=df_pivot_años_group.columns[2:])

# Salida: El DataFrame que contiene los valores escalados de las columnas seleccionadas
st.dataframe(df_pivot_años_group_escaler)

#---------------------------------------------------------------------------------

num_clusters = 5

kmeans = KMeans(n_clusters=num_clusters, random_state=42)

kmeans.fit(df_pivot_años_group_escaler)

cluster_labels = kmeans.labels_

df_cluster = df_pivot_años_group_escaler.assign(Cluster=cluster_labels)

st.dataframe(df_cluster)

#------------------------------------------------------------------------

df_pivot_años_group_cluster = df_pivot_años_group.assign(Cluster=cluster_labels)
df_pivot_años_group_cluster

df_pivot_años_group_cluster['Cluster'] = df_pivot_años_group_cluster['Cluster'].astype("str")
# Configurar el estilo de seaborn
sns.set(style="whitegrid")

# Crear la aplicación Streamlit
st.title("Visualización de Datos con Streamlit")

# Visualizar el gráfico de caja con seaborn
st.subheader("Gráfico de Caja por Clúster")
fig, ax = plt.subplots(figsize=(10, 6))
sns.boxplot(data=df_pivot_años_group_cluster, y='Life expectancy at birth, total (years)', x='Cluster', ax=ax)
st.pyplot(fig)


#------------------------------------------------------------------------------

# Configurar la aplicación Streamlit
st.title("Visualización de Datos con Streamlit")

# Crear el histograma con seaborn
fig, ax = plt.subplots(figsize=(10, 6))
sns.histplot(data=df_pivot_años_group_cluster, x="Life expectancy at birth, total (years)", hue="Cluster", ax=ax)
ax.set_title("Histograma de Expectativa de Vida por Cluster")
ax.set_xlabel("Expectativa de Vida al Nacer (años)")
ax.set_ylabel("Frecuencia")

# Mostrar el histograma en Streamlit
st.pyplot(fig)

#-----------------------------------------------------------------------------------

# Configurar la aplicación Streamlit
st.title("Visualización de Datos con Streamlit")

# Crear el gráfico de caja con seaborn
fig, ax = plt.subplots(figsize=(10, 6))
sns.boxplot(data=df_pivot_años_group_cluster, y='GDP per capita (current US$)', x='Cluster')
ax.set_title("Gráfico de Caja de GDP per capita por Cluster")
ax.set_xlabel("Cluster")
ax.set_ylabel("GDP per capita (current US$)")

# Mostrar el gráfico de caja en Streamlit
st.pyplot(fig)

#------------------------------------------------------------------------------------


# Configurar la aplicación Streamlit
st.title("Visualización de Datos con Streamlit")

# Crear el histograma con seaborn
fig, ax = plt.subplots(figsize=(10, 6))
sns.histplot(data=df_pivot_años_group_cluster, x="GDP per capita (current US$)", hue="Cluster", ax=ax)
ax.set_title("Histograma de GDP per capita por Cluster")
ax.set_xlabel("GDP per capita (current US$)")
ax.set_ylabel("Frecuencia")

# Mostrar el histograma en Streamlit
st.pyplot(fig)

#------------------------------------------------------------------------------------
# Configurar la aplicación Streamlit
st.title("Visualización de Datos con Streamlit")

# Crear el gráfico de caja con seaborn
fig, ax = plt.subplots(figsize=(10, 6))
sns.boxplot(data=df_pivot_años_group_cluster, y='ratio_population ages 65 and above', x='Cluster')
ax.set_title("Gráfico de Caja de la Proporción de Población de 65 años y más por Cluster")
ax.set_xlabel("Cluster")
ax.set_ylabel("Proporción de Población de 65 años y más")

# Mostrar el gráfico de caja en Streamlit
st.pyplot(fig)

#------------------------------------------------------------------------------------

# Configurar la aplicación Streamlit
st.title("Visualización de Datos con Streamlit")

# Crear el histograma con seaborn
fig, ax = plt.subplots(figsize=(10, 6))
sns.histplot(data=df_pivot_años_group_cluster, x="ratio_population ages 65 and above", hue="Cluster", ax=ax)
ax.set_title("Histograma de la Proporción de Población de 65 años y más por Cluster")
ax.set_xlabel("Proporción de Población de 65 años y más")
ax.set_ylabel("Frecuencia")

# Mostrar el histograma en Streamlit
st.pyplot(fig)

#------------------------------------------------------------------------------------
# Configurar la aplicación Streamlit
st.title("Visualización de Datos con Streamlit")

# Crear el gráfico de dispersión con seaborn
fig, ax = plt.subplots(figsize=(10, 6))
sns.scatterplot(data=df_pivot_años_group_cluster, x="Life expectancy at birth, total (years)", y="GDP per capita (current US$)", hue="Cluster", ax=ax)
ax.set_title("Gráfico de Dispersión de Expectativa de Vida y GDP per capita por Cluster")
ax.set_xlabel("Expectativa de Vida al Nacer (años)")
ax.set_ylabel("GDP per capita (current US$)")

# Mostrar el gráfico de dispersión en Streamlit
st.pyplot(fig)

#--------------------------------------------------------------------------------------

cluster_2 = df_pivot_años_group_cluster[df_pivot_años_group_cluster["Cluster"]=="2"]
st.dataframe(cluster_2 )

#------------------------------------------------------------------------------------
valores = []

for i in range(2,11):

  modelo_kmeans_numcluster = KMeans(n_clusters = i, random_state = 123)
  modelo_kmeans_numcluster.fit(df_pivot_años_group_escaler)
  valores.append(modelo_kmeans_numcluster.inertia_)


# Configurar la aplicación Streamlit
st.title("Visualización de Datos con Streamlit")

# Crear el gráfico de líneas con seaborn
fig, ax = plt.subplots(figsize=(10, 6))
sns.lineplot(x=range(2, 11), y=valores, marker="o", ax=ax)
ax.set_title("Gráfico de Líneas")
ax.set_xlabel("Eje X")
ax.set_ylabel("Eje Y")

# Mostrar el gráfico de líneas en Streamlit
st.pyplot(fig)
#------------------------------------------------------------------------------------


scores = []

for i in range(2,11):
  modelo_kmeans_numcluster = KMeans(n_clusters = i, random_state = 123)
  modelo_kmeans_numcluster.fit(df_pivot_años_group_escaler)
  etiquetas = modelo_kmeans_numcluster.labels_
  coef_silhouette = silhouette_score(df_pivot_años_group_escaler, etiquetas)
  scores.append(coef_silhouette)
# Configurar la aplicación Streamlit
st.title("Visualización de Datos con Streamlit")

# Crear el gráfico de líneas con seaborn
fig, ax = plt.subplots(figsize=(10, 6))
sns.lineplot(x=range(2, 11), y=scores, marker="o", ax=ax)
ax.set_title("Gráfico de Líneas")
ax.set_xlabel("Eje X")
ax.set_ylabel("Puntuaciones")

# Mostrar el gráfico de líneas en Streamlit
st.pyplot(fig)

#------------------------------------------------------------------------------------