import pandas as pd
from google.cloud import storage
from io import StringIO

def transform_indicator_to_row(request):
    """
    Cloud Function que transforma los indicadores de Banco Mundial en formato ancho a formato largo.

    Args:
        request (flask.Request): La solicitud HTTP.

    Returns:
        str: Mensaje de confirmación.
    """
    
    # Nombre del archivo en el bucket
    bucket_name = 'api-data-banco-mundial'
    blob_name = 'banco_mundial_data.csv'

    # Cargar el archivo desde el bucket a un DataFrame
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_text()

    # Crear el DataFrame
    df = pd.read_csv(StringIO(data))

    # Cambiar el nombre de las columnas 'economy' y 'time'
    df = df.rename(columns={'economy': 'Country', 'time': 'Year'})

    # Quedarse con los últimos 4 dígitos en la columna 'Year'
    df['Year'] = df['Year'].str[-4:]

    # Reorganizar el DataFrame pasando al indicador a nivel de registro en lugar de columna
    df = pd.melt(df, id_vars=['Country', 'Year'], var_name='Indicator', value_name='Value')

    # Guardar el DataFrame transformado en un archivo CSV en una ubicación temporal
    df.to_csv('/tmp/banco_mundial_data_indicator_to_row.csv', index=False)

    # Nombre del archivo de salida en el bucket
    blob_name_output = 'banco_mundial_data_indicator_to_row.csv'

    # Sube el archivo al mismo bucket
    blob_output = bucket.blob(blob_name_output)
    blob_output.upload_from_filename('/tmp/banco_mundial_data_indicator_to_row.csv')

    #retorno
    return 'Transformación exitosa y archivo guardado en el bucket.'