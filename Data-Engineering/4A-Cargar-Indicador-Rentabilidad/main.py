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


def registrar_auditoria(bq_client, registros_leidos, registros_insertados, registros_modificados):
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
            'Proceso': 'cargar_indicador_rentabilidad',
            'Tabla': 'IndicadorRentabilidad',
            'RegLeidos': registros_leidos,
            'RegInsertados': registros_insertados,
            'RegActualizados': registros_modificados,
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


def obtener_maximo_id_rentabilidad(bq_client, conjunto_datos, tabla):
    """
    Obtiene el máximo valor de IdIndicadorRentabilidad de la tabla IndicadorRentabilidad en BigQuery.

    Args:
        bq_client (google.cloud.bigquery.Client): Cliente de BigQuery.
        conjunto_datos (str): Nombre del conjunto de datos.
        tabla (str): Nombre de la tabla.

    Returns:
        int: El máximo valor de IdIndicadorRentabilidad.
    """
    try:
        query = f'''
            SELECT MAX(IdIndicadorRentabilidad) as max_id
            FROM `{conjunto_datos}.{tabla}`
        '''
        query_job = bq_client.query(query)
        result = query_job.result()

        max_id = 0
        for row in result:
            max_id = row.max_id

        return max_id if max_id is not None else 0
    except Exception as e:
        raise Exception(f"Error al obtener el máximo IdIndicadorRentabilidad: {str(e)}")


def cargar_indicador_rentabilidad(request):
    """
    Genera categorías de rentabilidad en BigQuery basándose en un archivo CSV de configuración.

    Args:
        request (flask.Request): La solicitud HTTP que desencadena la ejecución de la función.

    Returns:
        str: Un mensaje de éxito indicando el resultado de la operación.
    """
    try:
        # Configurar el cliente de BigQuery
        bq_client = bigquery.Client()

        # Nombre del conjunto de datos y tabla de BigQuery
        conjunto_datos = 'Modelo_Esperanza_De_Vida'
        tabla = 'IndicadorRentabilidad'

        # Nombre del archivo CSV en Cloud Storage
        bucket_name = 'pf-henry-esperanza-parametros'
        blob_name = 'Parametros_Indicador_Rentabilidad.csv'

        # Descargar el archivo CSV desde Cloud Storage a Pandas DataFrame
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Verificar existencia del archivo
        if not blob.exists():
            raise Exception(f"El archivo '{blob_name}' no existe en el bucket '{bucket_name}'.")

        file_content = blob.download_as_text()
        df_rentabilidad = pd.read_csv(StringIO(file_content))

        # Obtener el máximo IdIndicadorRentabilidad de la tabla actual
        max_id_rentabilidad = obtener_maximo_id_rentabilidad(bq_client, conjunto_datos, tabla)

        # Estructura de la tabla en BigQuery
        schema = [
            bigquery.SchemaField('IdIndicadorRentabilidad', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('IndicadorRentabilidad', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('Descripcion', 'STRING', mode='REQUIRED'),
        ]

        # Crear una referencia al conjunto de datos y la tabla de BigQuery
        dataset_ref = bq_client.dataset(conjunto_datos)
        table_ref = dataset_ref.table(tabla)

        # Configuración del trabajo de carga
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition='WRITE_APPEND',  # Agregar a la tabla existente
        )

        # Inicializar listas para registros de insert y update
        registros_insert = []
        registros_update = []

        # Iterar sobre filas del DataFrame
        for _, row in df_rentabilidad.iterrows():
            # Incrementar el máximo IdIndicadorRentabilidad
            max_id_rentabilidad += 1

            # Crear un diccionario con los datos del registro
            nuevo_registro = {
                'IdIndicadorRentabilidad': max_id_rentabilidad,
                'IndicadorRentabilidad': row['IndicadorRentabilidad'],
                'Descripcion': row['Descripcion'],
            }

            # Verificar si el IndicadorRentabilidad ya existe en la tabla
            query = f'''
                SELECT *
                FROM `{conjunto_datos}.{tabla}`
                WHERE IndicadorRentabilidad = {row['IndicadorRentabilidad']}
            '''
            query_job = bq_client.query(query)
            result = query_job.result()

            # Agregar a la lista correspondiente (insert o update)
            if result.total_rows == 0:
                registros_insert.append(nuevo_registro)
            else:
                # Verificar si hay cambios en la descripción
                for r in result:
                    if r['Descripcion'] != row['Descripcion']:
                        registros_update.append(nuevo_registro)

        # Realizar la operación de insert
        if registros_insert:
            bq_client.insert_rows_json(table_ref, registros_insert)

        # Realizar la operación de update
        if registros_update:
            for reg in registros_update:
                bq_client.update_rows(table_ref, [reg], ['Descripcion'])

        # Registra en la tabla de Auditoria después de cargar Continentes
        registros_auditoria = registrar_auditoria(bq_client, len(df_rentabilidad), len(registros_insert), len(registros_update))

        # Mostrar estadísticas
        print("*--------------------------------------------*")
        print("*                                            *")
        print("*          Estadísticas de Ejecución         *")
        print("*                                            *")
        print("*--------------------------------------------*")
        print("*")
        print(f'Registros leídos del CSV: {len(df_rentabilidad)}')
        print(f'Registros insertados    : {len(registros_insert)}')
        print(f'Registros modificados   : {len(registros_update)}')
        print("*")
        print(f'Registros de Auditoria Generados: {registros_auditoria}')

        return f'Generación de categorías de rentabilidad exitosa.'
    except Exception as e:
        # Registra el mensaje de error
        print(f"Error en la generación de categorías de rentabilidad: {str(e)}")
        return f'Error en la generación de categorías de rentabilidad. {str(e)}'
