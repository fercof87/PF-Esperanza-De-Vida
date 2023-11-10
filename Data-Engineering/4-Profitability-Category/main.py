import pandas as pd
from google.cloud import storage
from constantes import PROFIT_DICT

def generar_categorias_rentabilidad(request):
    """
    Genera el archivo CSV con las categorías de rentabilidad y lo guarda en un bucket de Google Cloud Storage.

    Esta función realiza las siguientes operaciones:
    1. Crea un DataFrame con las categorías de rentabilidad.
    2. Guarda el DataFrame en un archivo CSV local con codificación UTF-8.
    3. Sube el archivo CSV al bucket especificado en Google Cloud Storage.

    Args:
        request (flask.Request): La solicitud HTTP que desencadena la ejecución de la función.
    
    Returns:
        str: Un mensaje de éxito indicando que el archivo se generó con éxito y se guardó en el bucket.
    """
    # Nombre del archivo de salida en el bucket para categorías de rentabilidad
    bucket_name = 'api-data-banco-mundial'
    blob_name = 'Categorias_Rentabilidad.csv'

    # Subir el archivo al bucket de Google Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob_rentabilidad = bucket.blob(blob_name)

    # Crear el DataFrame de categorías de rentabilidad
    df_rentabilidad = pd.DataFrame(list(PROFIT_DICT.items()), columns=['ID_Categoria_Rentabilidad', 'Descripcion'])

    # Guardar el DataFrame en un archivo CSV local con codificación UTF-8
    df_rentabilidad.to_csv('/tmp/Categorias_Rentabilidad.csv', index=False, encoding='utf-8')

    # Subir el archivo CSV al bucket de Google Cloud Storage
    blob_rentabilidad.upload_from_filename('/tmp/Categorias_Rentabilidad.csv')

    return 'Archivo de Categorías de Rentabilidad generado con éxito y guardado en el bucket.'
