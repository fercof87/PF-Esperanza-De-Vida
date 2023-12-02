import os
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io

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
        return max_nro

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
    

def registrar_auditoria(bq_client):
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
            'Proceso': 'eliminar_duplicados_parametros',
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
    


def eliminar_duplicados_parametros(request):
    """
    Elimina duplicados de archivos en el bucket 'pf-henry-esperanza-parametros'.

    Args:
        request (flask.Request): La solicitud HTTP.

    Raises:
        Exception: Si se produce un error durante el proceso.

    Returns:
        str: Un mensaje indicando si la operación fue exitosa.
    """

    try:
        print("Iniciando la función eliminar_duplicados_parametros.")

        # Nombre del bucket
        bucket_name = "pf-henry-esperanza-parametros"

        # Nombres de los archivos a procesar
        archivos_a_procesar = ["Parametros_Paises.csv", "Parametros_Indicadores.csv", "Parametros_Indicador_Rentabilidad.csv"]

        # Crear el cliente de almacenamiento
        storage_client = storage.Client()

        # Variable para rastrear la cantidad de duplicados eliminados por archivo
        duplicados_eliminados_por_archivo = {}

        # Iterar sobre cada archivo
        for archivo in archivos_a_procesar:
            print(f"Procesando el archivo: {archivo}")

            # Verificar si el archivo existe antes de intentar procesarlo
            if not storage_client.bucket(bucket_name).blob(archivo).exists():
                print(f"Advertencia: El archivo {archivo} no existe en el bucket.")
                continue

            # Leer el archivo desde el bucket
            blob = storage_client.bucket(bucket_name).blob(archivo)
            file_content = blob.download_as_text()
            df = pd.read_csv(io.StringIO(file_content))

            # Obtener la cantidad de duplicados antes de eliminarlos
            duplicados_originales = df.shape[0] - df.drop_duplicates().shape[0]

            # Eliminar duplicados del DataFrame
            df_sin_duplicados = df.drop_duplicates()

            # Sobrescribir el archivo en el bucket
            nuevo_contenido = df_sin_duplicados.to_csv(index=False)
            blob.upload_from_string(nuevo_contenido, content_type='text/csv')

            # Actualizar la variable de duplicados eliminados por archivo
            duplicados_eliminados_por_archivo[archivo] = duplicados_originales

            print(f"Eliminados duplicados y sobrescrito en el bucket: {archivo}")

        # Mostrar la cantidad de duplicados eliminados por archivo
        for archivo, cantidad_eliminada in duplicados_eliminados_por_archivo.items():
            print(f"Duplicados eliminados en {archivo}: {cantidad_eliminada}")

        # inserción en tabla de Auditoria
        bq_client = bigquery.Client()
        registrar_auditoria(bq_client)

        return "Proceso completado. Duplicados eliminados y archivos sobrescritos en el bucket."

    except Exception as e:
        # Lanzar una excepción en caso de error
        error_message = f"Error en eliminar_duplicados_parametros: {e}"
        print(error_message)
        raise Exception(error_message)
