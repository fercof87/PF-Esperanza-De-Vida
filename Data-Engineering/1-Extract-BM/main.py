import pandas as pd
import wbgapi as wb
from google.cloud import storage
from constantes import *

def extract_data(request):
    """
    Extrae datos de la API del Banco Mundial y los guarda en un bucket de Google Cloud Storage.

    Esta función realiza las siguientes operaciones:
    1. Descarga una lista de países de la API del Banco Mundial.
    2. Filtra la lista de países según una lista predefinida (COUNTRIES_LIST).
    3. Descarga datos económicos desde la API para indicadores predefinidos (INDICATORS_LIST).
    4. Guarda los datos en un archivo CSV local en /tmp/.
    5. Sube el archivo CSV al bucket especificado (bucket_name) en Google Cloud Storage.

    Args:
        request (flask.Request): La solicitud HTTP que desencadena la ejecución de la función.

    Returns:
        str: Un mensaje de éxito indicando que la extracción se realizó con éxito y el archivo se guardó en el bucket.
    """
    # Descargar datos de la API del Banco Mundial
    # Obtenemos una lista de países de la API
    countries = wb.economy.list()

    # Creamos un DataFrame con la lista de países filtrando por COUNTRIES_LIST
    df_countries = pd.DataFrame(countries)
    df_countries = df_countries[df_countries['id'].isin(COUNTRIES_LIST)][['id', 'value', 'region', 'longitude', 'latitude']].reset_index(drop=True)

    # Descargamos datos económicos desde la API para los indicadores definidos
    df_wdi = wb.data.DataFrame(INDICATORS_LIST, COUNTRIES_LIST, mrv=36, columns="series").reset_index()

    # Guardar datos en un archivo CSV local
    df_wdi.to_csv('/tmp/banco_mundial_data.csv', index=False)

    # Nombre del archivo de salida en el bucket
    blob_name = 'banco_mundial_data.csv'
    bucket_name = 'api-data-banco-mundial'

    # Subir el archivo al bucket de Google Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename('/tmp/banco_mundial_data.csv')

    # Retornar un mensaje de éxito
    return 'Extracción exitosa y archivo guardado en el bucket.'


