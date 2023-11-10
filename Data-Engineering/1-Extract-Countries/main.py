import pandas as pd
import wbgapi as wb
from google.cloud import storage
from opencage.geocoder import OpenCageGeocode
from constantes import COUNTRIES_LIST, CONTINENTS_DICT

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

def extract_countries(request):
    """
    Genera archivos de continentes y países y los guarda en un bucket de Google Cloud Storage.

    Esta función realiza las siguientes operaciones:
    1. Descarga una lista de países de la API del Banco Mundial.
    2. Renombra las columnas del DataFrame de países.
    3. Agrega la columna Continente al DataFrame de países.
    4. Guarda el DataFrame de países en un archivo CSV local y lo sube al bucket especificado.
    5. Crea y sube al bucket el DataFrame de Continentes en un archivo CSV local.

    Args:
        request (flask.Request): La solicitud HTTP que desencadena la ejecución de la función.
    Returns:
        str: Un mensaje de éxito indicando que los archivos se generaron con éxito y se guardaron en el bucket.
    """
    # Nombre del archivo de salida en el bucket para países
    bucket_name_countries = 'api-data-banco-mundial'
    blob_name_countries = 'Paises.csv'

    # Subir el archivo de países al bucket de Google Cloud Storage
    storage_client_countries = storage.Client()
    bucket_countries = storage_client_countries.bucket(bucket_name_countries)

    # Descargar datos de la API del Banco Mundial
    # Obtenemos una lista de países de la API
    countries = wb.economy.list()

    # Creamos un DataFrame con la lista de países filtrando por COUNTRIES_LIST
    df_countries = pd.DataFrame(countries)
    df_countries = df_countries[df_countries['id'].isin(COUNTRIES_LIST)][['id', 'value', 'region', 'latitude', 'longitude', 'capitalCity']].reset_index(drop=True)

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

    # Guardar el DataFrame de países en un archivo CSV local
    df_countries.to_csv('/tmp/Paises.csv', index=False, encoding='utf-8')

    # Subir el archivo de países al bucket de Google Cloud Storage
    blob_countries = bucket_countries.blob(blob_name_countries)
    blob_countries.upload_from_filename('/tmp/Paises.csv')

    # Nombre del archivo de salida en el bucket para Continentes
    blob_name_continentes = 'Continentes.csv'

    # Subir el archivo de Continentes al bucket de Google Cloud Storage
    blob_continentes = bucket_countries.blob(blob_name_continentes)

    # Crear el DataFrame de Continentes
    df_continentes = pd.DataFrame(list(CONTINENTS_DICT.items()), columns=['ID_Continente', 'Continente'])

    # Guardar el DataFrame de Continentes en un archivo CSV local
    df_continentes.to_csv('/tmp/Continentes.csv', index=False, encoding='utf-8')

    # Subir el archivo de Continentes al bucket de Google Cloud Storage
    blob_continentes.upload_from_filename('/tmp/Continentes.csv')

    # Verificar errores y retornar un mensaje de éxito
    if blob_countries.exists() and blob_continentes.exists():
        return 'Archivos de países y continentes generados con éxito y guardados en el bucket.'
    else:
        return 'Error al subir archivos al bucket.'
