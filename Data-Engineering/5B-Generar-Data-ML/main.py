import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import db_dtypes

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

def registrar_auditoria_generar_data_ml(bq_client, registros_leidos):
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
            'Proceso': 'generar_data_ml',
            'Tabla': None,
            'RegLeidos': registros_leidos,
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


def generar_data_ML(request):
    """
    Genera datos para ML a partir de una consulta en BigQuery y guarda el resultado en un archivo CSV.

    :param request: Datos de la solicitud (no utilizado en esta función).
    :return: Mensaje indicando si la generación fue exitosa.
    """
    try:
        # Configurar el cliente de BigQuery
        bq_client = bigquery.Client()

        # Ejecutar la consulta en BigQuery y convertir el resultado a un DataFrame de pandas
        df_result = bq_client.query('''
            SELECT p.Pais, c.Continente, di.Anio, di.Valor, i.Descripcion as Indicador
            FROM `possible-willow-403216.Modelo_Esperanza_De_Vida.DatosIndicador` as di
            JOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.Pais`           as p
              ON di.IdPais = p.IdPais
            JOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.Continente`     as c
              ON p.IdContinente = c.IdContinente
            JOIN `possible-willow-403216.Modelo_Esperanza_De_Vida.Indicador`      as i
              ON i.IdIndicador = di.IdIndicador
        ''').to_dataframe()

        # Verificar si el DataFrame tiene datos
        if df_result.empty:
            raise Exception("La consulta no devolvió datos.")

        # Exportar el DataFrame a un archivo CSV
        bucket_name = 'pf-henry-esperanza-mlops'
        csv_file_name = 'Data-ML.csv'
        df_result.to_csv(f'/tmp/{csv_file_name}', index=False, encoding='utf-8', sep=';')

        # Subir el archivo CSV al bucket de Google Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(csv_file_name)
        blob.upload_from_filename(f'/tmp/{csv_file_name}')

        # Verificar errores y retornar un mensaje de éxito
        if blob.exists():

            # Grabar Registro de Auditoria
            registros_auditoria = registrar_auditoria_generar_data_ml(bq_client, len(df_result))

            # Mostrar estadísticas
            print("*--------------------------------------------*")
            print("*                                            *")
            print("*          Estadísticas de Ejecución         *")
            print("*                                            *")
            print("*--------------------------------------------*")
            print("*")
            print(f' Total de Registros Leídos    : {len(df_result)}')
            print("*")
            print(f" Registros de Auditoria Generados: {registros_auditoria}")
            print("*")
            return 'Archivo Data-ML.csv generado con éxito y guardado en el bucket pf-henry-esperanza-mlops.'
        else:
            return 'Error al subir el archivo CSV al bucket pf-henry-esperanza-mlops.'

    except Exception as e:
        print(f"Error en la generación de datos para ML: {str(e)}")
        return 'Error en la generación de datos para ML.'

