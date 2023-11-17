import os
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd

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


def archivo_existe(storage_client, bucket_name, archivo):
    """
    Verifica la existencia de un archivo en un bucket de Google Cloud Storage.

    Args:
        storage_client (google.cloud.storage.Client): Cliente de almacenamiento.
        bucket_name (str): Nombre del bucket.
        archivo (str): Nombre del archivo.

    Returns:
        bool: True si el archivo existe, False si no existe.
    """
    # Verificar la existencia del archivo en el bucket
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(archivo)
    return blob.exists()


def obtener_maximo_nro_ejecucion(bq_client):
    """
    Obtiene el máximo NroEjecucion de la tabla de Auditoria en BigQuery.

    :param bq_client: Cliente de BigQuery.
    :return: El máximo NroEjecucion.
    """
    try:
        query = 'SELECT MAX(NroEjecucion) AS max_nro FROM `Modelo_Esperanza_De_Vida.Auditoria`'
        query_job = bq_client.query(query)
        results = query_job.result()
        row = next(results)
        max_nro = row['max_nro']
        
        if max_nro == None:
            return 1
        return max_nro + 1

    except Exception as e:
        raise Exception(f"Error al obtener el máximo NroEjecucion: {e}")


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


def registrar_auditoria_verificacion_archivos(bq_client):
    """
    Registra en la tabla de Auditoria después de verificar archivos.

    :param bq_client: Cliente de BigQuery.
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
            'Proceso': 'verificar_archivos',
            'Tabla': None,
            'RegLeidos': 0,
            'RegInsertados': 0,
            'RegActualizados': 0,
            'RegEliminados': 0,
        }

        # Convertir el diccionario a un DataFrame
        df = pd.DataFrame([auditoria_data])

        dataset_ref = bq_client.dataset("Modelo_Esperanza_De_Vida")
        table_ref = dataset_ref.table("Auditoria")

        job_config = bigquery.LoadJobConfig(
            schema=AUDITORIA_SCHEMA,
            write_disposition='WRITE_APPEND',
        )

        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

        print("Inserción en tabla Auditoria después de verificar archivos. Registros insertados: 1")
    except Exception as e:
        raise Exception(f"Error al insertar en tabla Auditoria después de verificar archivos: {e}")




def verificar_archivos(request):
    """
    Verifica la existencia de archivos en un bucket de Google Cloud Storage.

    Args:
        request (flask.Request): La solicitud HTTP.
    Returns:
        str: Un mensaje indicando si los archivos están presentes o no.
    Raises:
        Exception: Si hay archivos faltantes, se lanza una excepción con un mensaje detallado.
    """
    try:
        print("Iniciando la función verificar_archivos.")

        # Nombre del bucket
        bucket_name = "pf-henry-esperanza-parametros"

        # Nombres de los archivos a verificar
        archivos_a_verificar = ["Parametros_Paises.csv", "Parametros_Indicadores.csv"]

        # Crear el cliente de almacenamiento
        storage_client = storage.Client()

        # Verificar la existencia de cada archivo
        archivos_faltantes = [archivo for archivo in archivos_a_verificar if not archivo_existe(storage_client, bucket_name, archivo)]

        # Si hay archivos faltantes, lanzar una excepción
        if archivos_faltantes:
            raise Exception(f"Archivos {archivos_faltantes} en bucket {bucket_name}. !!! SE CANCELA EL PROCESO ETL !!!")


        # Si no hay archivos faltantes, continuar con el proceso
        print("Iniciando Proceso ETL...")

        # inserción en tabla de Auditoria
        bq_client = bigquery.Client()
        registrar_auditoria_verificacion_archivos(bq_client)

        return "Iniciando Proceso ETL..."
        
    except Exception as e:
        raise Exception(f"Error general en verificar_archivos: {e}")
