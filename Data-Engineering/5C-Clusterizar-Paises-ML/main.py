import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from io import StringIO


# Definir el esquema de la tabla Auditoria
AUDITORIA_SCHEMA = [
    bigquery.SchemaField('IdAuditoria', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('NroEjecucion', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('Entorno', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Proceso', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Tabla', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('RegLeidos', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('RegInsertados', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('RegActualizados', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('RegEliminados', 'INTEGER', mode='NULLABLE'),
]

# Años a Procesar
ANIOS_PROCESO = [2017,2018,2019,2020,2021]



def obtener_maximo_id_indicador(bq_client):
    """
    Obtiene el máximo IdIndicador de la tabla de Indicador en BigQuery.

    :param bq_client: Cliente de BigQuery.
    :return: El máximo IdIndicador.
    """
    try:
        query = 'SELECT MAX(IdIndicador) AS max_id FROM `Modelo_Esperanza_De_Vida.Indicador`'
        query_job = bq_client.query(query)
        results = query_job.result()
        row = next(results)
        max_id = row['max_id']

        return max_id if max_id is not None else 0

    except Exception as e:
        raise Exception(f"Error al obtener el máximo IdIndicador: {e}")
    

def insertar_nuevos_indicadores(bq_client, df_nuevos_indicadores):
    """
    Inserta nuevos registros en la tabla de Indicador.

    :param bq_client: Cliente de BigQuery.
    :param df_nuevos_indicadores: DataFrame con nuevos indicadores a insertar.
    :return: Número de registros insertados en la tabla.
    """
    try:
        # Obtener el máximo IdIndicador
        max_id_indicador = obtener_maximo_id_indicador(bq_client)

        # Incrementar IdIndicador para cada nuevo indicador
        df_nuevos_indicadores['IdIndicador'] = range(max_id_indicador + 1, max_id_indicador + 1 + len(df_nuevos_indicadores))

        # Insertar los nuevos indicadores en la tabla
        job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND')

        job = bq_client.load_table_from_dataframe(df_nuevos_indicadores, 'Modelo_Esperanza_De_Vida.Indicador', job_config=job_config)
        job.result()

        return len(df_nuevos_indicadores)

    except Exception as e:
        raise Exception(f"Error al insertar nuevos indicadores en la tabla de Indicador: {e}")
    

def obtener_id_indicadores_con_insercion(bq_client, lista_indicadores):
    """
    Obtiene los IdIndicador correspondientes a una lista de indicadores.
    Inserta nuevos indicadores si no se encuentran en la tabla de Indicador.

    :param bq_client: Cliente de BigQuery.
    :param lista_indicadores: Lista de nombres de indicadores.
    :return: Un diccionario con los nombres de los indicadores y sus IdIndicador correspondientes.
    """
    try:
        # Crear la consulta SQL para obtener los IdIndicador
        indicadores_query = f'SELECT Descripcion, IdIndicador FROM `Modelo_Esperanza_De_Vida.Indicador` WHERE Descripcion IN ({", ".join(["'" + ind + "'" for ind in lista_indicadores])})'

        # Ejecutar la consulta
        query_job = bq_client.query(indicadores_query)
        results = query_job.result()

        # Crear el diccionario de IdIndicador
        id_indicadores = {row['Descripcion']: row['IdIndicador'] for row in results}

        # Obtener las Descripciones no encontradas en la tabla
        descripciones_faltantes = set(lista_indicadores) - set(id_indicadores.keys())

        # Crear un DataFrame con las nuevas Descripciones
        df_nuevos_indicadores = pd.DataFrame({
            'CodIndicador': 'ML.OPS.CLUSTER',
            'Descripcion': list(descripciones_faltantes)
        })

        # Insertar nuevos indicadores si hay Descripciones faltantes
        if not df_nuevos_indicadores.empty:
            registros_insertados = insertar_nuevos_indicadores(bq_client, df_nuevos_indicadores)
            print(f"Se insertaron {registros_insertados} nuevos indicadores.")

        # Obtener los IdIndicador actualizados
        id_indicadores_actualizados = obtener_id_indicadores(bq_client, lista_indicadores)

        return id_indicadores_actualizados

    except Exception as e:
        raise Exception(f"Error al obtener IdIndicador con inserción: {e}")



def obtener_maximo_id_auditoria(bq_client):
    """
    Obtiene el máximo ID de auditoria de la tabla de Auditoria en BigQuery.

    :param bq_client: Cliente de BigQuery.
    :return: El máximo ID de auditoria.
    """
    try:
        query = 'SELECT MAX(IdAuditoria) AS max_id FROM `Modelo_Esperanza_De_Vida.Auditoria`'
        query_job = bq_client.query(query)
        results = query_job.result()
        row = next(results)
        max_id = row['max_id']

        return max_id if max_id is not None else 0
        
    except Exception as e:
        raise Exception(f"Error al obtener el máximo ID de auditoría: {e}")
        

def obtener_maximo_nro_ejecucion(bq_client):
    """
    Obtiene el máximo NroEjecucion de la tabla de Auditoria en BigQuery.

    :param bq_client: Cliente de BigQuery.
    :return: El máximo NroEjecucion.
    """
    try:
        query = 'SELECT MAX(NroEjecucion) AS max_nro FROM `possible-willow-403216.Modelo_Esperanza_De_Vida.Auditoria`'
        query_job = bq_client.query(query)
        results = query_job.result()
        row = next(results)
        max_nro = row['max_nro']
        
        return max_nro 

    except Exception as e:
        raise Exception(f"Error al obtener el máximo NroEjecucion: {e}")
    

def registrar_auditoria(bq_client, registros_leidos, registros_grabados,registros_actualizados):
    """
    Registra en la tabla de Auditoria después de generar datos para ML.

    :param bq_client: Cliente de BigQuery.
    :param registros_leidos: Número de registros leídos.
    """
    try:
        # Obtener el máximo IdAuditoria y NroEjecucion
        max_id_auditoria = obtener_maximo_id_auditoria(bq_client)
        max_nro_ejecucion = obtener_maximo_nro_ejecucion(bq_client)

        # Datos para la auditoría
        auditoria_data = {
            'IdAuditoria': max_id_auditoria + 1,
            'NroEjecucion': max_nro_ejecucion,
            'Entorno': 'Dag-Airflow-Composer',
            'Proceso': 'clusterizar_paises',
            'Tabla': 'ParametrosML',
            'RegLeidos': registros_leidos,
            'RegInsertados': registros_grabados,
            'RegActualizados': registros_actualizados,
            'RegEliminados': 0,
        }

        # Convertir el diccionario a un DataFrame
        df = pd.DataFrame([auditoria_data])

        # Insertar en la tabla de Auditoria
        dataset_ref = bq_client.dataset("Modelo_Esperanza_De_Vida")
        table_ref = dataset_ref.table("Auditoria")

        job_config = bigquery.LoadJobConfig(
            schema=AUDITORIA_SCHEMA,
            write_disposition='WRITE_APPEND',
        )

        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

        #Retorno la cantidad de Registros Insertados
        return df.shape[0]

    except Exception as e:
        raise Exception(f"Error al insertar en tabla Auditoria después de generar datos para ML: {e}")
    

def read_csv_from_bucket(bucket_name, file_name):
    """
    Lee un archivo CSV de un bucket de Cloud Storage y retorna un DataFrame de Pandas.

    Parameters:
    - bucket_name (str): Nombre del bucket de Cloud Storage.
    - file_name (str): Nombre del archivo CSV.

    Returns:
    pd.DataFrame: DataFrame de Pandas con los datos del archivo CSV.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()
    df = pd.read_csv(StringIO(content), delimiter=';')
    return df



def export_df_to_gcs(df, project_id, bucket_name, file_name):
    """
    Exporta un DataFrame a un archivo CSV en UTF-8 y lo guarda en un bucket de Google Cloud Storage.

    Parameters:
    - df (pd.DataFrame): DataFrame de pandas a exportar.
    - project_id (str): ID del proyecto de Google Cloud.
    - bucket_name (str): Nombre del bucket de Google Cloud Storage.
    - file_name (str): Nombre del archivo CSV en el bucket.

    Returns:
    None
    """
    try:
        # Convertir el DataFrame a formato CSV en memoria (StringIO)
        csv_content = StringIO()
        df.to_csv(csv_content, index=False, encoding='utf-8')
        csv_content.seek(0)

        # Crear un cliente de Storage
        client = storage.Client(project=project_id)

        # Obtener el bucket
        bucket = client.get_bucket(bucket_name)

        # Crear un nuevo blob (objeto) en el bucket
        blob = bucket.blob(file_name)

        # Subir el contenido del DataFrame al blob
        blob.upload_from_file(csv_content, content_type='text/csv')

        print(f"DataFrame exportado a gs://{bucket_name}/{file_name}")

    except Exception as e:
        print(f"Error durante la exportación a GCS: {e}")



def actualizar_parametros_ml(bq_client, df_parametros_ml):
    """
    Actualiza la tabla de Parametros con los nuevos valores.

    :param bq_client: Cliente de BigQuery.
    :param df_parametros_ml: DataFrame con los nuevos parámetros ML.
    :return: Número de registros actualizados en la tabla.
    """
    try:
        # Realizar el truncate de la tabla Parametros
        truncate_query = 'TRUNCATE TABLE `Modelo_Esperanza_De_Vida.ParametrosML`'
        bq_client.query(truncate_query).result()

        # Insertar los nuevos parámetros ML en la tabla
        job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND')

        job = bq_client.load_table_from_dataframe(df_parametros_ml, 'Modelo_Esperanza_De_Vida.ParametrosML', job_config=job_config)
        job.result()

        return len(df_parametros_ml)
    except Exception as e:
        raise Exception(f"Error al actualizar la tabla de Parametros: {e}")



def obtener_id_indicadores(bq_client, lista_indicadores):
    """
    Obtiene los IdIndicador correspondientes a una lista de indicadores.

    :param bq_client: Cliente de BigQuery.
    :param lista_indicadores: Lista de nombres de indicadores.
    :return: Un diccionario con los nombres de los indicadores y sus IdIndicador correspondientes.
    """
    try:
        # Crear la consulta SQL para obtener los IdIndicador
        indicadores_query = f'SELECT Descripcion, IdIndicador FROM `Modelo_Esperanza_De_Vida.Indicador` WHERE Descripcion IN ({", ".join(["'" + ind + "'" for ind in lista_indicadores])})'
        
        # Ejecutar la consulta
        query_job = bq_client.query(indicadores_query)
        results = query_job.result()

        # Crear el diccionario de IdIndicador
        id_indicadores = {row['Descripcion']: row['IdIndicador'] for row in results}

        return id_indicadores
    except Exception as e:
        raise Exception(f"Error al obtener IdIndicador: {e}")



def obtener_parametros_ml(df_auxiliar, lista_indicadores):
    """
    Obtiene los parámetros ML (ValorMin y ValorMax) para cada indicador.

    :param df_auxiliar: DataFrame con los datos de interés.
    :param lista_indicadores: Lista de nombres de indicadores.
    :return: Un DataFrame con los datos de parámetros ML.
    """
    try:
        # Crear una lista de diccionarios
        parametros_ml_list = []

        for indicador in lista_indicadores:
            valor_max = df_auxiliar[indicador].max()
            valor_min = df_auxiliar[indicador].min()

            # Agregar un diccionario a la lista
            parametros_ml_list.append({
                'Descripcion': indicador,
                'ValorMax': valor_max,
                'ValorMin': valor_min
            })

        # Convertir la lista de diccionarios en un DataFrame
        parametros_ml = pd.DataFrame(parametros_ml_list)

        return parametros_ml
    except Exception as e:
        raise Exception(f"Error al obtener parámetros ML: {e}")



def clusterizar_paises(request):
    """
    Cloud Function para realizar la clusterización de países por medio de ML.

    Parameters:
    - request: Requiere una solicitud HTTP, pero no se usa en la función.

    Returns:
    str: Mensaje de éxito o error en la generación del archivo Imputaciones_ML.csv.
    """
    try:
        # Configurar el cliente de BigQuery
        bq_client = bigquery.Client()

        # Parámetros de Cloud Storage
        project_id       = 'possible-willow-403216'
        bucket_name      = 'pf-henry-esperanza-mlops'
        file_name        = 'Data-ML.csv'
        file_name_dest   = 'imputaciones_ML.csv'
        df_data_ml       = read_csv_from_bucket(bucket_name, file_name)
        registros_leidos = df_data_ml.shape[0]
        
        #-------------------------------------------------------#
        #                          MLOPs
        #-------------------------------------------------------#
        df_data_ml.rename(columns={'Anio': 'Año'}, inplace=True)
        
        # Paso indicadores a nivel columna
        df_pivot = pd.pivot_table(data=df_data_ml, index=['Pais', "Año",'Continente'], columns='Indicador', values='Valor')
        df_pivot.reset_index(inplace=True)

        # Crear nuevas columnas
        df_pivot["ratio_population ages 65 and above"] = df_pivot["Population ages 65 and above, total"]/df_pivot["Population, total"]
        df_pivot["ratio_urban population"] = df_pivot["Urban population"]/df_pivot["Population, total"]

        # Seleccionar columnas específicas
        columnas_seleccionadas = [
            "Pais",
            "Año",
            "Continente",
            "GDP per capita (current US$)",
            "Inflation, GDP deflator (annual %)",
            "Inflation, consumer prices (annual %)",
            "Life expectancy at birth, total (years)",
            "Population growth (annual %)",
            "ratio_population ages 65 and above",
            "ratio_urban population",
            "Urban population growth (annual %)"
        ]
        df_pivot = df_pivot[columnas_seleccionadas]

        # Crear una lista nueva llamada lista_indicadores_ML
        lista_indicadores_ML = [ind for ind in columnas_seleccionadas if ind not in ['Pais', 'Año', 'Continente']]
        
        # Obtener los IdIndicador correspondientes
        #id_indicadores = obtener_id_indicadores(bq_client, lista_indicadores_ML)
        id_indicadores = obtener_id_indicadores_con_insercion(bq_client, lista_indicadores_ML)
        # Crear el DataFrame df_indicadores_ML
        df_indicadores_ML = pd.DataFrame(list(id_indicadores.items()), columns=['Descripcion', 'IdIndicador'])

        # Filtrar por años y llenar valores nulos con 0
        df_pivot_años = df_pivot[df_pivot["Año"].isin(ANIOS_PROCESO)].copy()
        df_pivot_años.reset_index(drop=True, inplace=True)
        df_pivot_años.fillna(0, inplace=True)

        # Agrupar por País y Continente, calcular la media y quitar la columna Año
        df_pivot_años_group = df_pivot_años.groupby(["Pais","Continente"]).mean().drop(columns=['Año'])
        df_pivot_años_group.reset_index(inplace=True)

        # Escalado
        scaler = StandardScaler()
        columnas = df_pivot_años_group.columns[2:]
        df_pivot_años_group_escaler = scaler.fit_transform(df_pivot_años_group[columnas])
        df_pivot_años_group_escaler = pd.DataFrame(df_pivot_años_group_escaler, columns=df_pivot_años_group.columns[2:])
        df_escaler = df_pivot_años_group_escaler

        # Clusterizacion
        num_clusters = 5
        kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init=10)
        #kmeans = KMeans(n_clusters=num_clusters, random_state=42)
        kmeans.fit(df_escaler)
        cluster_labels = kmeans.labels_
        df_cluster = df_escaler.assign(Cluster=cluster_labels)
        df_cluster = df_cluster.groupby("Cluster").mean()
        df_pivot_años_group_cluster = df_pivot_años_group.assign(Cluster=cluster_labels)
        df_pivot_años_group_cluster['Cluster'] = df_pivot_años_group_cluster['Cluster'].astype("str")
        
        # Selección de cluster y filtrado
        df_paises_rentables = df_pivot_años_group_cluster[df_pivot_años_group_cluster["Cluster"]=="2"]['Pais']

        # Exportar a GCP el dataframe
        export_df_to_gcs(df_paises_rentables, project_id, bucket_name, file_name_dest)

        # Calcular ValorMin y ValorMax para cada indicador
        df_auxiliar = df_pivot_años_group_cluster[df_pivot_años_group_cluster["Cluster"] == "2"]
        #df_indicadores_ML[['ValorMin', 'ValorMax']] = obtener_parametros_ml(df_auxiliar, lista_indicadores_ML)
        df_parametros_ml = obtener_parametros_ml(df_auxiliar, lista_indicadores_ML)  
        
        # Fusionar DataFrames
        df_indicadores_ML = pd.merge(df_indicadores_ML, df_parametros_ml, on='Descripcion', how='left')
        
        # Reorganizar las columnas
        df_indicadores_ML = df_indicadores_ML[['IdIndicador', 'ValorMin', 'ValorMax']]

        # Actualizar la tabla Parametros con los nuevos valores
        registros_actualizados = actualizar_parametros_ml(bq_client, df_indicadores_ML)

        # Registrar auditoria
        registros_auditoria = registrar_auditoria(bq_client, registros_leidos, df_paises_rentables.shape[0],registros_actualizados)

        # Mostrar estadísticas
        print("*--------------------------------------------*")
        print("*                                            *")
        print("*          Estadísticas de Ejecución         *")
        print("*                                            *")
        print("*--------------------------------------------*")
        print("*")
        print(f' Total de Registros Leídos    : {registros_leidos}')
        print("*")
        print(f' Total de Países Rentables    : {df_paises_rentables.shape[0]}')
        print("*")
        print(f' Total de ParametrosML        : {registros_actualizados}')
        print("*")
        print(f" Registros de Auditoria Generados: {registros_auditoria}")
        print("*")

        return f"Se Ha Generado el archivo {file_name_dest} en el bucket {bucket_name} correctamente."

    except Exception as e:
        print(f"Error en la generación del archivo {file_name_dest} en el bucket {bucket_name}: {str(e)}")
        return f"Error en la generación del archivo {file_name_dest} en el bucket {bucket_name}."
