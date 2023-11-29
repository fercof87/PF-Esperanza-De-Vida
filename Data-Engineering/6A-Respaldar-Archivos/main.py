import os
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import Conflict
from datetime import datetime
from tempfile import NamedTemporaryFile


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

def registrar_auditoria(bq_client):
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
            'Proceso': 'respaldar_archivos',
            'Tabla': None,
            'RegLeidos': 0,
            'RegInsertados': 0,
            'RegActualizados': 0,
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
    

def mover_renombrar_archivos(storage_client, old_bucket, filename, new_bucket):
    """
    Mueve y renombra un archivo de un bucket a otro en Google Cloud Storage.

    Parameters:
    - storage_client (google.cloud.storage.Client): Sesión de Google Cloud Storage.
    - old_bucket (str): Nombre del bucket de origen.
    - filename (str): Nombre del archivo a mover y renombrar.
    - new_bucket (str): Nombre del bucket de destino.

    Returns:
    - bool: True si el archivo fue movido y renombrado exitosamente, False si el archivo no existe en el bucket de origen.
    """
    # Verificar si el archivo existe en el bucket de origen
    blob = storage_client.bucket(old_bucket).blob(filename)
    if not blob.exists():
        print(f"El archivo '{filename}' no existe en el bucket de origen '{old_bucket}'. No se puede mover.")
        return False

    # Construir el nuevo nombre del archivo con la fecha
    fecha_actual = datetime.now().strftime("%d-%m-%Y")
    nuevo_nombre = f"{filename.split('.')[0]}-{fecha_actual}.csv"

    try:
        # Descargar el contenido del blob a un archivo temporal
        with NamedTemporaryFile(delete=False) as temp_file:
            blob.download_to_filename(temp_file.name)

            # Crear una copia del archivo en el nuevo bucket con el nuevo nombre
            new_blob = storage_client.bucket(new_bucket).blob(nuevo_nombre)
            new_blob.upload_from_filename(temp_file.name)

        # Eliminar el archivo temporal y el archivo original si la copia fue exitosa
        os.remove(temp_file.name)
        blob.delete()

        print(f"Archivo '{filename}' movido y renombrado a '{new_bucket}/{nuevo_nombre}'.")
        return True
    except Conflict:
        print(f"Ya existe un archivo con el nombre '{nuevo_nombre}' en el bucket de destino '{new_bucket}'."
                      " Cambie el nombre del archivo antes de intentar moverlo.")
        return False
    except Exception as e:
        print(f"Error inesperado: {str(e)}")
        return False


def respaldar_archivos(request):
    """
    Respaldar archivos de parámetros e intermedios en Google Cloud Storage.

    Parameters:
    - request: Parámetro de solicitud (puede ser cualquier cosa, ya que no se utiliza).

    Returns:
    - str: Mensaje indicando que los archivos fueron respaldados con éxito.
    """
    # Configurar el cliente de BigQuery
    bq_client = bigquery.Client()
  
    # Nombres de los buckets
    bucket_name_parametros  = 'pf-henry-esperanza-parametros'
    bucket_name_intermedios = 'pf-henry-esperanza-archivos-intermedios'
    bucket_name_respaldos   = 'pf-henry-esperanza-respaldos'
    bucket_name_mlops       = 'pf-henry-esperanza-mlops'

    # Archivos a respaldar en cada bucket
    files_to_bkp_parametros     = ['Parametros_Paises.csv', 'Parametros_Indicador_Rentabilidad.csv', 'Parametros_Indicadores.csv']
    files_to_bkp_intermedios    = ['Paises.csv', 'banco_mundial_data.csv', 'banco_mundial_data_a_registros.csv']
    files_to_bkp_mlops          = ['Data-ML.csv', 'imputaciones_ML.csv']

    # Crear la sesión de Google Cloud Storage
    storage_client = storage.Client()

    # Estadísticas de archivos movidos
    archivos_movidos_origen = {}
    archivos_movidos_destino = {}

    # Respaldar los archivos de Parámetros de la Ejecución actual
    for file_name in files_to_bkp_parametros:
        if mover_renombrar_archivos(storage_client, bucket_name_parametros, file_name, bucket_name_respaldos):
            archivos_movidos_origen[bucket_name_parametros] = archivos_movidos_origen.get(bucket_name_parametros, []) + [file_name]
            archivos_movidos_destino[bucket_name_respaldos] = archivos_movidos_destino.get(bucket_name_respaldos, []) + [f"{file_name.split('.')[0]}-{datetime.now().strftime('%d-%m-%Y')}.csv"]

    # Respaldar los archivos intermedios de la Ejecución actual
    for file_name in files_to_bkp_intermedios:
        if mover_renombrar_archivos(storage_client, bucket_name_intermedios, file_name, bucket_name_respaldos):
            archivos_movidos_origen[bucket_name_intermedios] = archivos_movidos_origen.get(bucket_name_intermedios, []) + [file_name]
            archivos_movidos_destino[bucket_name_respaldos] = archivos_movidos_destino.get(bucket_name_respaldos, []) + [f"{file_name.split('.')[0]}-{datetime.now().strftime('%d-%m-%Y')}.csv"]

    # Respaldar los archivos de MLOps de la Ejecución actual
    for file_name in files_to_bkp_mlops:
        if mover_renombrar_archivos(storage_client, bucket_name_mlops, file_name, bucket_name_respaldos):
            archivos_movidos_origen[bucket_name_mlops] = archivos_movidos_origen.get(bucket_name_mlops, []) + [file_name]
            archivos_movidos_destino[bucket_name_respaldos] = archivos_movidos_destino.get(bucket_name_respaldos, []) + [f"{file_name.split('.')[0]}-{datetime.now().strftime('%d-%m-%Y')}.csv"]

    # Registrar auditoria
    registros_auditoria = registrar_auditoria(bq_client)
    
    # Mostrar estadísticas
    print("*--------------------------------------------*")
    print("*                                            *")
    print("*          Estadísticas de Ejecución         *")
    print("*                                            *")
    print("*--------------------------------------------*")

    # Estadísticas de archivos movidos desde buckets de origen
    print("\n1) Archivos movidos desde los buckets de origen:")
    for bucket, archivos in archivos_movidos_origen.items():
        print(f"\n  {bucket}:\n    - Archivos: {archivos}")
    print("*")
    
    # Estadísticas de archivos movidos al bucket de destino
    print("\n2) Archivos movidos al bucket de destino:")
    for bucket, archivos in archivos_movidos_destino.items():
        print(f"\n  {bucket}:\n    - Archivos: {archivos}")
    print("*")

    print(f" Registros de Auditoria Generados: {registros_auditoria}")
    
    
    return 'Archivos respaldados con éxito.'
