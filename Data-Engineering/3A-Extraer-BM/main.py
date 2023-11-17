import pandas as pd
from io import StringIO
import wbgapi as wb
from google.cloud import storage
from google.cloud import bigquery


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


def registrar_auditoria(bq_client, registros_leidos):
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
            'Proceso': 'extraer_datos_BM',
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
        raise Exception(f"Error al insertar en tabla Auditoria : {e}")


def leer_parametros_paises():
    """
    Lee el archivo CSV 'Parametros_Paises.csv' desde el bucket 'api-data-banco-mundial' en Google Cloud Storage.

    Returns:
        list: Una lista con los países leídos desde el archivo CSV.
    
    Raises:
        Exception: Si ocurre un error al leer el archivo o el archivo no existe.
    """
    try:
        # Crear una instancia del cliente de Google Cloud Storage
        client = storage.Client()

        # Especifica el nombre del bucket y el nombre del archivo CSV
        bucket_name = 'pf-henry-esperanza-parametros'
        file_name = 'Parametros_Paises.csv'

        # Obtiene una referencia al bucket y al archivo
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Lee el contenido del archivo CSV
        content = blob.download_as_text()

        # Parsea el contenido CSV a una lista
        df = pd.read_csv(StringIO(content))
        paises_list = df['CodPais'].tolist()

        return paises_list
    
    except Exception as e:
        raise Exception(f"Error al leer el archivo 'Parametros_Paises.csv': {str(e)}")
    

def leer_parametros_indicadores():
    """
    Lee el archivo CSV 'Parametros_Indicadores.csv' desde el bucket 'api-data-banco-mundial' en Google Cloud Storage.

    Returns:
        list: Una lista con los indicadores leídos desde el archivo CSV.
    
    Raises:
        Exception: Si ocurre un error al leer el archivo o el archivo no existe.
    """
    try:
        # Crear una instancia del cliente de Google Cloud Storage
        client = storage.Client()

        # Especifica el nombre del bucket y el nombre del archivo CSV
        bucket_name = 'pf-henry-esperanza-parametros'
        file_name = 'Parametros_Indicadores.csv'

        # Obtiene una referencia al bucket y al archivo
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Lee el contenido del archivo CSV
        content = blob.download_as_text()

        # Parsea el contenido CSV a una lista
        df = pd.read_csv(StringIO(content))
        indicadores_list = df['CodIndicador'].tolist()

        return indicadores_list
    
    except Exception as e:
        raise Exception(f"Error al leer el archivo 'Parametros_Indicadores.csv': {str(e)}")
    
def extraer_datos_BM(request):
    """
    Descarga datos desde la API del Banco Mundial y guarda el archivo CSV en un bucket de Google Cloud Storage.

    Esta función realiza las siguientes operaciones:
    1. Descarga datos económicos desde la API para indicadores predefinidos (INDICATORS_LIST).
    2. Guarda los datos en un archivo CSV local y lo sube al bucket especificado.

    Args:
        request (flask.Request): La solicitud HTTP que desencadena la ejecución de la función.
    Returns:
        str: Un mensaje de éxito indicando que la extracción se realizó con éxito y el archivo se guardó en el bucket.
    """
    # Configurar el cliente de BigQuery
    bq_client = bigquery.Client()

    # Nombre del archivo de salida en el bucket para indicadores
    bucket_name_wdi = 'pf-henry-esperanza-archivos-intermedios'
    blob_name_wdi = 'banco_mundial_data.csv'

    # Subir el archivo de indicadores al bucket de Google Cloud Storage
    storage_client_wdi = storage.Client()
    bucket_wdi = storage_client_wdi.bucket(bucket_name_wdi)


    # Realiza la lectura del archivo de Países a procesar en la ejecución
    lista_paises = leer_parametros_paises()
    
    # Realiza la lectura del archivo de Indicadores  a procesar en la ejecución
    lista_indicadores = leer_parametros_indicadores()
    
    # Descargamos datos económicos desde la API para los indicadores definidos
    df_wdi = wb.data.DataFrame(lista_indicadores, lista_paises, mrv=36, columns="series").reset_index()
    registros_extraidos = df_wdi.shape[0]

    # Guardar datos en un archivo CSV local para indicadores con encoding UTF-8
    df_wdi.to_csv('/tmp/banco_mundial_data.csv', index=False, encoding='utf-8')

    # Subir el archivo de indicadores al bucket de Google Cloud Storage
    blob_wdi = bucket_wdi.blob(blob_name_wdi)
    blob_wdi.upload_from_filename('/tmp/banco_mundial_data.csv')

    # Registra en la tabla de Auditoria después de cargar Continentes
    registros_auditoria = registrar_auditoria(bq_client, registros_extraidos)


    # Mostrar estadísticas
    print("*--------------------------------------------*")
    print("*                                            *")
    print("*          Estadísticas de Ejecución         *")
    print("*                                            *")
    print("*--------------------------------------------*")
    print("*")
    print(f' Total de registros Extraidos del BM: {registros_extraidos}')
    print("*")
    print(f" Registros de Auditoria Generados   : {registros_auditoria}")
    print("*")

    return 'Carga completada de Continentes en BigQuery.'


    # Verificar errores y retornar un mensaje de éxito
    if blob_wdi.exists():
        return 'Extracción exitosa del BM y archivo guardado en el bucket.'
    else:
        return 'Error al subir el archivo al bucket.'
