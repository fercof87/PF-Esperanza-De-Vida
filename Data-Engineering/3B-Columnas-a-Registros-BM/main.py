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


def registrar_auditoria_cargar_continente(bq_client, registros_leidos):
    """
    Registra en la tabla de Auditoria.

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
            'Proceso': 'transformar_columnas_a_registros_BM',
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
        raise Exception(f"Error al insertar en tabla Auditoria: {e}")


def transformar_columnas_a_registros_BM(request):
    """
    Cloud Function que transforma los indicadores de Banco Mundial en formato ancho a formato largo.

    Args:
        request (flask.Request): La solicitud HTTP.

    Returns:
        str: Mensaje de confirmación.
    """
    # Configurar el cliente de BigQuery
    bq_client = bigquery.Client()

    # Nombre del archivo en el bucket
    bucket_name = 'pf-henry-esperanza-archivos-intermedios'
    blob_name = 'banco_mundial_data.csv'

    # Cargar el archivo desde el bucket a un DataFrame
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_text()

    # Crear el DataFrame
    df = pd.read_csv(StringIO(data))

    # Cambiar el nombre de las columnas 'economy' y 'time'
    df = df.rename(columns={'economy': 'CodPais', 'time': 'Año'})

    # Quedarse con los últimos 4 dígitos en la columna 'Year'
    df['Año'] = df['Año'].str[-4:]

    # Reorganizar el DataFrame pasando al indicador a nivel de registro en lugar de columna
    df = pd.melt(df, id_vars=['CodPais', 'Año'], var_name='Indicador', value_name='Valor')
    registros_leidos = df.shape[0]

    # Guardar el DataFrame transformado en un archivo CSV en una ubicación temporal
    df.to_csv('/tmp/banco_mundial_data_a_registros.csv', index=False, encoding='utf-8')

    # Nombre del archivo de salida en el bucket
    blob_name_output = 'banco_mundial_data_a_registros.csv'

    # Sube el archivo al mismo bucket
    blob_output = bucket.blob(blob_name_output)
    blob_output.upload_from_filename('/tmp/banco_mundial_data_a_registros.csv')

    # Registra en la tabla de Auditoria después de cargar Continentes
    registros_auditoria = registrar_auditoria_cargar_continente(bq_client, registros_leidos)

    # Mostrar estadísticas
    print("*--------------------------------------------*")
    print("*                                            *")
    print("*          Estadísticas de Ejecución         *")
    print("*                                            *")
    print("*--------------------------------------------*")
    print("*")
    print(f' Total de registros leídos    : {registros_leidos}')
    print("*")
    print(f" Registros de Auditoria Generados: {registros_auditoria}")
    print("*")

    #retorno
    return 'Transformación exitosa y archivo guardado en el bucket.'