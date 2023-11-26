import streamlit as st
import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

#logo
st.sidebar.image('Fixing Data.jpg', caption='Proyecto Final')
#---------------------------------------------------------------
#titulo
st.title('Modelo Machine Learning')

#-------------------------------------------------------------------------

df = pd.read_csv('data_ML.csv')
num_clusters = 5

kmeans = KMeans(n_clusters=num_clusters, random_state=42)

kmeans.fit(df)

cluster_labels = kmeans.labels_

df_cluster = df.assign(Cluster=cluster_labels)
st.dataframe(df_cluster)

df_pivot_años_group_escaler_cluster = df_pivot_años_group_escaler_cluster.groupby("Cluster").mean()

df_pivot_años_group_escaler_cluster

df_pivot_años_group_cluster = df_pivot_años_group.assign(Cluster=cluster_labels)
df_pivot_años_group_cluster

