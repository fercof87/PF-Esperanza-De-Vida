import os
import logging
from google.cloud import storage
from google.cloud.exceptions import Conflict
from datetime import datetime
from tempfile import NamedTemporaryFile


def setup_logging():
    logging.basicConfig(level=logging.INFO)


def mover_renombrar_archivos(storage_client, old_bucket, filename, new_bucket):
    """
    Mueve y renombra un archivo de un bucket a otro en Google Cloud Storage.

    Parameters:
    - storage_client (google.cloud.storage.Client): Sesión de Google Cloud Storage.
    - old_bucket (str): Nombre del bucket de origen.
    - filename (str): Nombre del archivo a mover y renombrar.
    - new_bucket (str): Nombre del bucket de destino.

    Returns:
    - bool: True si el archivo fue movido y renombrado exitosamente, False si el archivo no existe en el bucket de origen.
    """
    # Verificar si el archivo existe en el bucket de origen
    blob = storage_client.bucket(old_bucket).blob(filename)
    if not blob.exists():
        logging.error(f"El archivo '{filename}' no existe en el bucket de origen '{old_bucket}'. No se puede mover.")
        return False

    # Construir el nuevo nombre del archivo con la fecha
    fecha_actual = datetime.now().strftime("%d-%m-%Y")
    nuevo_nombre = f"{filename.split('.')[0]}-{fecha_actual}.csv"

    try:
        # Descargar el contenido del blob a un archivo temporal
        with NamedTemporaryFile(delete=False) as temp_file:
            blob.download_to_filename(temp_file.name)

            # Crear una copia del archivo en el nuevo bucket con el nuevo nombre
            new_blob = storage_client.bucket(new_bucket).blob(nuevo_nombre)
            new_blob.upload_from_filename(temp_file.name)

        # Eliminar el archivo temporal y el archivo original si la copia fue exitosa
        os.remove(temp_file.name)
        blob.delete()

        logging.info(f"Archivo '{filename}' movido y renombrado a '{new_bucket}/{nuevo_nombre}'.")
        return True
    except Conflict:
        logging.error(f"Ya existe un archivo con el nombre '{nuevo_nombre}' en el bucket de destino '{new_bucket}'."
                      " Cambie el nombre del archivo antes de intentar moverlo.")
        return False
    except Exception as e:
        logging.error(f"Error inesperado: {str(e)}")
        return False


def respaldar_archivos(request):
    """
    Respaldar archivos de parámetros e intermedios en Google Cloud Storage.

    Parameters:
    - request: Parámetro de solicitud (puede ser cualquier cosa, ya que no se utiliza).

    Returns:
    - str: Mensaje indicando que los archivos fueron respaldados con éxito.
    """
    # Configurar el sistema de logging
    setup_logging()

    # Nombres de los buckets
    bucket_name_parametros = 'pf-henry-esperanza-parametros'
    bucket_name_intermedios = 'pf-henry-esperanza-archivos-intermedios'
    bucket_name_respaldos = 'pf-henry-esperanza-respaldos'

    # Archivos a respaldar en cada bucket
    files_to_bkp_parametros = ['Parametros_Paises.csv', 'Parametros_Indicador_Rentabilidad.csv', 'Parametros_Indicadores.csv']
    files_to_bkp_intermedios = ['Paises.csv', 'banco_mundial_data.csv', 'banco_mundial_data_a_registros.csv']

    # Crear la sesión de Google Cloud Storage
    storage_client = storage.Client()

    # Estadísticas de archivos movidos
    archivos_movidos_origen = {}
    archivos_movidos_destino = {}

    # Respaldar los archivos de Parámetros de la Ejecución actual
    for file_name in files_to_bkp_parametros:
        if mover_renombrar_archivos(storage_client, bucket_name_parametros, file_name, bucket_name_respaldos):
            archivos_movidos_origen[bucket_name_parametros] = archivos_movidos_origen.get(bucket_name_parametros, []) + [file_name]
            archivos_movidos_destino[bucket_name_respaldos] = archivos_movidos_destino.get(bucket_name_respaldos, []) + [f"{file_name.split('.')[0]}-{datetime.now().strftime('%d-%m-%Y')}.csv"]

    # Respaldar los archivos intermedios de la Ejecución actual
    for file_name in files_to_bkp_intermedios:
        if mover_renombrar_archivos(storage_client, bucket_name_intermedios, file_name, bucket_name_respaldos):
            archivos_movidos_origen[bucket_name_intermedios] = archivos_movidos_origen.get(bucket_name_intermedios, []) + [file_name]
            archivos_movidos_destino[bucket_name_respaldos] = archivos_movidos_destino.get(bucket_name_respaldos, []) + [f"{file_name.split('.')[0]}-{datetime.now().strftime('%d-%m-%Y')}.csv"]

    # Mostrar estadísticas
    logging.info("*--------------------------------------------*")
    logging.info("*                                            *")
    logging.info("*          Estadísticas de Ejecución         *")
    logging.info("*                                            *")
    logging.info("*--------------------------------------------*")

    # Estadísticas de archivos movidos desde buckets de origen
    logging.info("\n1) Archivos movidos desde los buckets de origen:")
    for bucket, archivos in archivos_movidos_origen.items():
        logging.info(f"\n  {bucket}:\n    - Archivos: {archivos}")

    # Estadísticas de archivos movidos al bucket de destino
    logging.info("\n2) Archivos movidos al bucket de destino:")
    for bucket, archivos in archivos_movidos_destino.items():
        logging.info(f"\n  {bucket}:\n    - Archivos: {archivos}")

    return 'Archivos respaldados con éxito.'
