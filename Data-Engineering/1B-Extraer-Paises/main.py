import csv
import pandas as pd
import wbgapi as wb
from google.cloud import storage
from google.cloud import bigquery
from opencage.geocoder import OpenCageGeocode

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
    Registra en la tabla de Auditoria después de cargar los Continentes.

    :param bq_client: Cliente de BigQuery.
    :param registros_leidos: Número de registros leídos.
    :param registros_insertados: Número de registros insertados.
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
            'Proceso': 'extrear_paises',
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
        raise Exception(f"Error al insertar en tabla Auditoria después de cargar Continentes: {e}")

        
def obtener_id_continentes():
    """
    Descarga la tabla 'Modelo_Esperanza_De_Vida.Continente' y retorna un diccionario
    con la relación entre Continente e IdContinente.
    
    Returns:
        dict: Un diccionario donde las claves son los nombres de continentes y los valores son los IdContinentes.
    """
    try:
        # Configurar el cliente de BigQuery
        bq_client = bigquery.Client()

        # Nombre del conjunto de datos y tabla de BigQuery para Continentes
        conjunto_datos_continentes = 'Modelo_Esperanza_De_Vida'
        tabla_continentes = 'Continente'

        # Consulta SQL para obtener todos los registros de la tabla
        query = f'''
            SELECT Continente, IdContinente
            FROM `{conjunto_datos_continentes}.{tabla_continentes}`
        '''

        # Ejecutar la consulta
        query_job = bq_client.query(query)
        result = query_job.result()

        # Crear un diccionario con la relación Continente - IdContinente
        id_continentes_dict = {row['Continente']: row['IdContinente'] for row in result}

        return id_continentes_dict
    except Exception as e:
        print(f"Error al obtener IdContinentes: {str(e)}")
        return {}
    

def leer_parametros_paises():
    """
    Lee el archivo CSV 'Parametros_Paises.csv' en Google Cloud Storage.

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
        paises_list = []
        csv_reader = csv.reader(content.splitlines())
        header = next(csv_reader)  # Ignorar la primera fila (encabezado)
        for row in csv_reader:
            paises_list.append(row[0])  # Suponiendo que el código del país está en la primera columna

        return paises_list
    except Exception as e:
        raise Exception(f"Error al leer el archivo 'Parametros_Paises.csv': {str(e)}")
    

def obtener_continente_por_pais(pais):
    """
    Obtiene el nombre del continente dado el nombre de un país.

    Args:
        pais (str): El nombre del país.
        
    Returns:
        str: El nombre del continente , o 'Sin Continente' si no se puede determinar.
    """
    api_key = '5ac3f8c024b2468dbd0a2e165f2246e1'
    geocoder = OpenCageGeocode(api_key)

    try:
        results = geocoder.geocode(query=pais)
        
        if results and len(results) > 0:
            nombre_continente = results[0]['components']['continent']

            # Realizar reemplazos específicos
            if nombre_continente    == 'South America':
                nombre_continente   = 'Sudamérica'
            elif nombre_continente  == 'North America':
                nombre_continente   = 'América del Norte'
            elif nombre_continente  == 'Europe':
                nombre_continente   = 'Europa'
            
            return nombre_continente
        else:
            return 'Sin Continente'
    except Exception as e:
        return f"Error al obtener el continente para {pais}: {str(e)}"


def extraer_paises(request):
    try:
        # Configurar el cliente de BigQuery
        bq_client = bigquery.Client()

        # Nombre del conjunto de datos y tabla de BigQuery para Continentes
        conjunto_datos_continentes = 'Modelo_Esperanza_De_Vida'
        tabla_continentes = 'Continente'

        # Descargar datos de la API del Banco Mundial
        countries = wb.economy.list()

        # Generar una lista con los países a procesar
        lista_paises = leer_parametros_paises()

        # Crear un DataFrame con la lista de países filtrando por COUNTRIES_LIST
        df_countries = pd.DataFrame(countries)
        df_countries = df_countries[df_countries['id'].isin(lista_paises)][['id', 'value', 'region', 'latitude', 'longitude', 'capitalCity']].reset_index(drop=True)

        # Renombrar las columnas del DataFrame de países
        df_countries = df_countries.rename(columns={
            'id': 'CodPais',
            'value': 'Pais',
            'region': 'Region',
            'latitude': 'Latitud',
            'longitude': 'Longitud',
            'capitalCity': 'Capital'
        })

        # Modificar la columna "Pais" para quedarse con el texto antes de la primera coma
        df_countries['Pais'] = df_countries['Pais'].str.split(',').str[0]

        # Agregar la columna Continente al DataFrame de países
        df_countries['Continente'] = df_countries['Pais'].map(obtener_continente_por_pais)

        # Descargamos los continentes
        id_continentes_dict = obtener_id_continentes()

        # buscamos los IdContinentes para cada Continente
        df_countries['IdContinente'] = df_countries['Continente'].map(id_continentes_dict)

        # Ordenar el DataFrame de países
        df_countries = df_countries[['IdContinente', 'CodPais', 'Pais', 'Region', 'Latitud', 'Longitud', 'Capital']]

        # Guardar el DataFrame de países en un archivo CSV local
        df_countries.to_csv('/tmp/Paises.csv', index=False, encoding='utf-8')

        # Subir el archivo de países al bucket de Google Cloud Storage
        storage_client_countries = storage.Client()
        bucket_countries = storage_client_countries.bucket('pf-henry-esperanza-archivos-intermedios')
        blob_countries = bucket_countries.blob('Paises.csv')
        blob_countries.upload_from_filename('/tmp/Paises.csv')

        # Verificar errores y retornar un mensaje de éxito
        if blob_countries.exists():

            #Grabar Registro de Auditoria
            registros_auditoria = registrar_auditoria_cargar_continente(bq_client, len(lista_paises))

            # Mostrar estadísticas
            print("*--------------------------------------------*")
            print("*                                            *")
            print("*          Estadísticas de Ejecución         *")
            print("*                                            *")
            print("*--------------------------------------------*")
            print("*")
            print(f' Total de Paises de Parametros: {len(lista_paises)}')
            print(f' Total de Paises Grabados     : {df_countries.shape[0]}')
            print("*")
            print(f" Registros de Auditoria Generados: {registros_auditoria}")
            print("*")
            return 'Archivos de Países.csv generados con éxito y guardados en el bucket pf-henry-esperanza-archivos-intermedios.'
        else:
            return 'Error al subir archivos al bucket pf-henry-esperanza-archivos-intermedios.'

    except Exception as e:
        print(f"Error en la extracción de países: {str(e)}")
        return 'Error en la extracción de países.'
