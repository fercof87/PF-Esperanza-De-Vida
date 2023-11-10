import pandas as pd
import wbgapi as wb
from google.cloud import storage
from constantes import COUNTRIES_LIST, INDICATORS_LIST

def extract_data(request):
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
    # Nombre del archivo de salida en el bucket para indicadores
    bucket_name_wdi = 'api-data-banco-mundial'
    blob_name_wdi = 'banco_mundial_data.csv'

    # Subir el archivo de indicadores al bucket de Google Cloud Storage
    storage_client_wdi = storage.Client()
    bucket_wdi = storage_client_wdi.bucket(bucket_name_wdi)

    # Descargamos datos económicos desde la API para los indicadores definidos
    df_wdi = wb.data.DataFrame(INDICATORS_LIST, COUNTRIES_LIST, mrv=36, columns="series").reset_index()

    # Guardar datos en un archivo CSV local para indicadores con encoding UTF-8
    df_wdi.to_csv('/tmp/banco_mundial_data.csv', index=False, encoding='utf-8')

    # Subir el archivo de indicadores al bucket de Google Cloud Storage
    blob_wdi = bucket_wdi.blob(blob_name_wdi)
    blob_wdi.upload_from_filename('/tmp/banco_mundial_data.csv')

    # Verificar errores y retornar un mensaje de éxito
    if blob_wdi.exists():
        return 'Extracción exitosa del BM y archivo guardado en el bucket.'
    else:
        return 'Error al subir el archivo al bucket.'
