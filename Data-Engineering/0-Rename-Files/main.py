from google.cloud import storage

def rename_files(request):
    """
    Renombra archivos en un bucket de Google Cloud Storage agregando '_ant' al final del nombre.

    Esta función toma una lista de nombres de archivos ('Paises.csv', 'banco_mundial_data.csv', 'Continentes.csv')
    y verifica si existen en un bucket especificado. Si existen, copia el contenido de cada archivo a un nuevo archivo
    con '_ant' agregado al final del nombre y luego elimina el archivo original.

    Args:
        request (flask.Request): La solicitud HTTP que desencadena la ejecución de la función.

    Returns:
        str: Un mensaje indicando que los archivos fueron renombrados con éxito en el bucket especificado.
    """
    bucket_name = 'api-data-banco-mundial'  # Reemplaza con el nombre de tu bucket
    files_to_rename = ['Paises.csv', 'banco_mundial_data.csv', 'Continentes.csv']

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for file_name in files_to_rename:
        blob = bucket.blob(file_name)
        if blob.exists():
            new_blob_name = file_name.replace('.csv', '_ant.csv')
            new_blob = bucket.blob(new_blob_name)

            # Copiar contenido al nuevo blob
            blob.download_to_filename('/tmp/tempfile.csv')
            new_blob.upload_from_filename('/tmp/tempfile.csv')

            # Eliminar el blob original
            blob.delete()

    return f'Archivos renombrados con éxito en el bucket {bucket_name}.'
