import io
from google.cloud import bigquery
from google.cloud import storage
from constantes import CONTINENTS_DICT
import pandas as pd

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


def registrar_auditoria_cargar_continente(bq_client, registros_leidos, registros_insertados):
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
            'Proceso': 'cargar_continente',
            'Tabla': 'Continente',
            'RegLeidos': registros_leidos,
            'RegInsertados': registros_insertados,
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


def obtener_max_id_continente(bq_client, conjunto_datos, tabla_continentes):
    try:
        # Consultar el máximo IdContinente presente en la tabla Continente
        query = f'''
            SELECT MAX(IdContinente) AS max_id
            FROM `{conjunto_datos}.{tabla_continentes}`
        '''
        query_job = bq_client.query(query)
        result = query_job.result()

        for row in result:
            return row['max_id']

    except Exception as e:
        print(f"Error al obtener el máximo IdContinente: {str(e)}")


def obtener_continentes_actuales(bq_client, conjunto_datos, tabla_continentes):
    try:
        # Consultar los continentes actuales en la tabla Continente
        query = f'''
            SELECT Continente
            FROM `{conjunto_datos}.{tabla_continentes}`
        '''
        query_job = bq_client.query(query)
        result = query_job.result()

        return [row['Continente'] for row in result]

    except Exception as e:
        print(f"Error al obtener los continentes actuales: {str(e)}")
        return []


def insertar_elementos(bq_client, conjunto_datos, tabla, elementos):
    """
    Inserta elementos en una tabla de BigQuery.

    :param bq_client: Cliente de BigQuery.
    :param conjunto_datos: Nombre del conjunto de datos.
    :param tabla: Nombre de la tabla.
    :param elementos: Lista de elementos a insertar.
    :raises: Exception si hay errores al insertar elementos.
    """
    try:
        # Insertar elementos en la tabla
        table_ref = bq_client.dataset(conjunto_datos).table(tabla)
        table = bq_client.get_table(table_ref)
        errors = bq_client.insert_rows(table, elementos, selected_fields=table.schema)

        if errors:
            raise Exception(f"Error al insertar elementos en la tabla {tabla}: {errors}")

    except Exception as e:
        print(f"Error al insertar elementos: {str(e)}")
        raise
    



def cargar_continente(request):
    """
    Cloud Function que carga la información de los Continentes en BigQuery.

    Esta función realiza las siguientes acciones:
    1. Configura el cliente de BigQuery.
    2. Obtiene el máximo IdContinente presente en la tabla Continente.
    3. Si la tabla está vacía, establece el máximo IdContinente en 0.
    4. Obtiene los continentes actuales en la tabla Continente.
    5. Itera sobre los continentes del diccionario CONTINENTS_DICT.
    6. Prepara los elementos a insertar si no están en la tabla.
    7. Incrementa el IdContinente para cada nuevo elemento a insertar.
    8. Inserta los elementos en la tabla Continente.
    9. Imprime los totales de registros leídos e insertados al final del proceso.
    10. Graba Datos de Auditoria

    :param request: Datos de la solicitud (no utilizado en esta función).
    :return: Mensaje indicando si la carga fue exitosa o si ocurrió un error.
    """
    try:
        # Configurar el cliente de BigQuery
        bq_client = bigquery.Client()

        # Nombre del conjunto de datos y tabla de BigQuery para Continentes
        conjunto_datos = 'Modelo_Esperanza_De_Vida'
        tabla_continentes = 'Continente'

        # Obtener el máximo IdContinente presente en la tabla Continente
        max_id_continente = obtener_max_id_continente(bq_client, conjunto_datos, tabla_continentes)
        # Si la tabla estaba vacía...
        if max_id_continente is None:
            max_id_continente = 0

        # Obtener los continentes actuales en la tabla Continente
        continentes_actuales = obtener_continentes_actuales(bq_client, conjunto_datos, tabla_continentes)

        # Inicializar la lista de elementos a insertar
        elementos_a_insertar = []

        # Inicializar contadores
        registros_leidos = 0
        registros_insertados = 0

        # Iterar sobre los continentes y preparar los elementos a insertar si no están en la tabla
        for continente, traducciones in CONTINENTS_DICT.items():
            registros_leidos += 1
            if continente not in continentes_actuales:
                max_id_continente += 1  # Incrementar el IdContinente
                elemento = {
                    'IdContinente': max_id_continente,
                    'Continente': continente,
                    'ContinenteEng': traducciones['ContinenteEng']
                }
                elementos_a_insertar.append(elemento)
                registros_insertados += 1

        # Insertar los elementos en la tabla Continente si hay registros para insertar
        if elementos_a_insertar:
            insertar_elementos(bq_client, conjunto_datos, tabla_continentes, elementos_a_insertar)

        # Registra en la tabla de Auditoria después de cargar Continentes
        registros_auditoria = registrar_auditoria_cargar_continente(bq_client, registros_leidos, registros_insertados)

        # Mostrar estadísticas
        print("*--------------------------------------------*")
        print("*                                            *")
        print("*          Estadísticas de Ejecución         *")
        print("*                                            *")
        print("*--------------------------------------------*")
        print("*")
        print(f' Proceso de Carga de Tabla    : {tabla_continentes}')
        print("*")
        print(f' Total de registros leídos    : {registros_leidos}')
        print(f' Total de registros insertados: {registros_insertados}')
        print("*")
        print(f" Registros de Auditoria Generados: {registros_auditoria}")
        print("*")
        return 'Carga completada de Continentes en BigQuery.'

    except Exception as e:
        print(f"Error en la carga de Continentes: {str(e)}")
        return 'Error en la carga de Continentes.'
