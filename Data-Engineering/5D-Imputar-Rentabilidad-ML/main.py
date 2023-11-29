import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
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
    

def registrar_auditoria(bq_client, registros_leidos, registros_actualizados):
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
            'Proceso': 'imputar_rentabilidad',
            'Tabla': 'Pais',
            'RegLeidos': registros_leidos,
            'RegInsertados': 0,
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
    df = pd.read_csv(StringIO(content))
    return df


def get_id_indicador_rentabilidad(client, project_id, dataset_id, table_id):
    """
    Obtiene el IdIndicadorRentabilidad correspondiente a la descripción "Rentable".

    Parameters:
    - project_id (str): ID del proyecto de Google Cloud.
    - dataset_id (str): ID del conjunto de datos de BigQuery.
    - table_id (str): ID de la tabla de BigQuery.

    Returns:
    int: ID del indicador de rentabilidad.
    """

    query = f"SELECT IdIndicadorRentabilidad FROM `{project_id}.{dataset_id}.{table_id}` WHERE Descripcion = 'Rentable'"
    result = client.query(query).result()

    # Obtener todos los resultados
    rows = list(result)

    if not rows:
        raise ValueError("No ha encontrado el indicador de rentabilidad 'RENTABLE'.")

    # Recuperar el primer resultado directamente de la lista
    row = rows[0]
    id_indicador_rentabilidad = row["IdIndicadorRentabilidad"]
    
    return id_indicador_rentabilidad



def update_bigquery_table(client, project_id, dataset_id, table_id, df, id_indicador_rentabilidad):
    """
    Actualiza la tabla Pais de BigQuery con la información del DataFrame.

    Parameters:
    - client (google.cloud.bigquery.Client): Cliente de BigQuery.
    - project_id (str): ID del proyecto de Google Cloud.
    - dataset_id (str): ID del conjunto de datos de BigQuery.
    - table_id (str): ID de la tabla de BigQuery.
    - df (pd.DataFrame): DataFrame de Pandas con los datos a actualizar.
    - id_indicador_rentabilidad (int): ID del indicador de rentabilidad a asignar.

    Returns:
    int: Cantidad de registros actualizados.
    """

    # Construir la consulta SELECT para obtener la cantidad de registros a actualizar
    count_query = f"""
    SELECT COUNT(IdPais) as num_rows
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE Pais IN ({", ".join([f'"{pais}"' for pais in df['Pais']])})
    """

    # Ejecutar la consulta SELECT
    count_result = client.query(count_query).result()
    num_rows_to_update = next(count_result)['num_rows']

    # Construir la consulta UPDATE
    update_query = f"""
    UPDATE `{project_id}.{dataset_id}.{table_id}`
    SET IdIndicadorRentabilidad = {id_indicador_rentabilidad}
    WHERE Pais IN ({", ".join([f'"{pais}"' for pais in df['Pais']])})
    """
    # Ejecutar la consulta UPDATE
    update_result = client.query(update_query).result()

    return num_rows_to_update


def imputar_rentabilidad(request):
    """
    Función principal que realiza la imputación del indicador de rentabilidad en BigQuery.
    El indicador de rentabilidad es un código que indica si un país es rentable o no, 
    como resultado del modelo de Clusters de ML.

    Parameters:
    - request (flask.Request): La solicitud de la función (no se utiliza directamente).

    Returns:
    None
    """
    try:
        # Descargamos CSV generado por el modelo de ML con los países Rentables
        bucket_name = 'pf-henry-esperanza-mlops'
        file_name = 'imputaciones_ML.csv'
        df_imputaciones = read_csv_from_bucket(bucket_name, file_name)

        # Parámetros  de Big Query
        project_id = 'possible-willow-403216'
        dataset_id = 'Modelo_Esperanza_De_Vida'
        table_id_rentabilidad = 'IndicadorRentabilidad'
        table_pais = 'Pais'

        #Creamos cliente
        client = bigquery.Client()

        # obtenemos el indicador de rentabilidad correspondiente para "Rentable"
        id_indicador_rentabilidad = get_id_indicador_rentabilidad(client, project_id, dataset_id, table_id_rentabilidad)

        # Imputamos los valores a los países del CSV
        registros_actualizados = update_bigquery_table(client, project_id, dataset_id, table_pais, df_imputaciones, id_indicador_rentabilidad)

        # grabamos registro de auditoria
        registros_auditoria = registrar_auditoria(client, df_imputaciones.shape[0], registros_actualizados)

        # Mostrar estadísticas
        print("*--------------------------------------------*")
        print("*                                            *")
        print("*          Estadísticas de Ejecución         *")
        print("*                                            *")
        print("*--------------------------------------------*")
        print("*")
        print(f' Total de Registros Leídos    : {df_imputaciones.shape[0]}')
        print("*")
        print(f' Total de Países Imputados    : {registros_actualizados}')
        print("*")
        print(f" Registros de Auditoria Generados: {registros_auditoria}")
        print("*")

        return 'Se Han imputado los Indicadores de Rentabilidad Correctamente en tabla País.'

    except Exception as e:
        print(f"Error en la imputación de Indicador Rentabilidad: {str(e)}")
        return 'Error en la imputación de Indicador Rentabilidad.'
