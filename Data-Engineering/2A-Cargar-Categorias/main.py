import pandas as pd
import wbgapi as wb
import io
from google.cloud import bigquery
from google.cloud import storage

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
            'Proceso': 'cargar_categorias',
            'Tabla': 'Categoria',
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


def obtener_proximo_id_categoria(bq_client, conjunto_datos, tabla):
    """
    Obtiene el próximo ID de categoría disponible en una tabla de BigQuery.

    :param bq_client: Cliente de BigQuery.
    :param conjunto_datos: Nombre del conjunto de datos.
    :param tabla: Nombre de la tabla.
    :return: El próximo ID de categoría disponible.
    """
    query = f'SELECT MAX(IdCategoria) AS max_id FROM `{conjunto_datos}.{tabla}`'
    query_job = bq_client.query(query)
    results = query_job.result()
    row = next(results)
    max_id = row['max_id']

    if max_id is None:
        return 1
    else:
        return max_id + 1


def insertar_categorias_en_bigquery(bq_client, conjunto_datos, tabla, df, id_inicial):
    """
    Inserta categorías en una tabla de BigQuery a partir de un DataFrame.

    :param bq_client: Cliente de BigQuery.
    :param conjunto_datos: Nombre del conjunto de datos.
    :param tabla: Nombre de la tabla.
    :param df: DataFrame con las categorías a insertar.
    :param id_inicial: ID inicial para las categorías.
    """
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('IdCategoria', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('Categoria', 'STRING', mode='REQUIRED'),
        ]
    )

    df['IdCategoria'] = range(id_inicial, id_inicial + len(df))

    job = bq_client.load_table_from_dataframe(
        df,
        bq_client.dataset(conjunto_datos).table(tabla),
        job_config=job_config
    )

    job.result()



def buscar_categorias(lista_indicadores):
    """
    Busca y devuelve las categorías de los indicadores en base a la lista de indicadores proporcionada.

    :param lista_indicadores: Lista de códigos de indicadores.
    :return: Un DataFrame con las categorías de los indicadores.
    """
    cod_ind = []
    name_ind = []

    for indicator in lista_indicadores:
        ind = wb.series.get(indicator)
        cod_ind.append(ind["id"])
        name_ind.append(ind["value"])

    # Crear un diccionario con las listas
    dict_indicators = {
        "cod_indicator": cod_ind,
        "name_indicator": name_ind
    }

    # Crear un dataframe con el diccionario
    df_indicators = pd.DataFrame(dict_indicators)

    topic_id = 1
    indicators_code = []
    category_id = []
    i = 0

    while topic_id <= 21:
        indicators_by_topic = wb.series.info(topic=topic_id)
        for item in indicators_by_topic.items:
            indicator_id = indicators_by_topic.items[i]['id']
            indicator_info = indicators_by_topic.items[i]['value']
            
            if indicator_id in lista_indicadores:
                indicators_code.append(indicator_id)
                category_id.append(topic_id)
            
            i += 1

        topic_id += 1
        i = 0

    # Crear un diccionario
    dict_category_ind = {
        "indicators_code": indicators_code,
        "category_id": category_id
    }

    df_indicator_category_id = pd.DataFrame(dict_category_ind)

    # Guardar todos los tópicos en una variable
    topics = {row['value']: row['id'] for row in wb.topic.list()}

    # Crear la lista category y llenarla con las claves
    category = []
    for element in topics.keys():
        category.append(element)

    # Crear la lista topic_ids y llenarla con los ids
    topic_ids = []
    for value in topics.values():
        topic_ids.append(value)

    # Crear un diccionario con las listas de id y category
    dict_category = {
        "category_id": topic_ids,
        "category": category
    }
    df_category = pd.DataFrame(dict_category)

    # Cambiar el tipo de dato de la columna id, para hacer el merge
    df_category["category_id"] = df_category["category_id"].astype(int)

    # Unir dataframes para visualizar código indicador, el id de la categoría(tópico) y su nombre
    df_indicator_category = df_indicator_category_id.merge(df_category, on="category_id")

    #return df_indicator_category[['category']].rename(columns={'category': 'Categoria'})
    return df_indicator_category[['category','category_id']].rename(columns={'category': 'Categoria', 'category_id': 'NroTopicoBM'})


def cargar_categorias(request):
    """
    Carga las categorías de indicadores en BigQuery a partir de un archivo CSV.

    :param request: Datos de la solicitud (no utilizado en esta función).
    :return: Mensaje indicando si la carga fue exitosa.
    """
    bq_client = bigquery.Client()
    proyecto = 'possible-willow-403216'
    conjunto_datos = 'Modelo_Esperanza_De_Vida'
    tabla = 'Categoria'

    storage_client = storage.Client()
    bucket_name = 'pf-henry-esperanza-parametros'
    indicadores_blob_name = 'Parametros_Indicadores.csv'

    bucket = storage_client.get_bucket(bucket_name)
    indicadores_blob = bucket.blob(indicadores_blob_name)

    # Verificar si el archivo existe en el bucket
    if not indicadores_blob.exists():
        raise Exception(f'Error: El archivo {indicadores_blob_name} no se encuentra en el bucket {bucket_name}.')


    indicadores_content = indicadores_blob.download_as_text()

    df_indicadores = pd.read_csv(io.StringIO(indicadores_content))
    
    df_categorias = buscar_categorias(df_indicadores['CodIndicador'].to_list())
    df_categorias = df_categorias.drop_duplicates(subset=['Categoria'])

    categorias = [f"'{categoria.replace("'", "''")}'" for categoria in df_categorias["Categoria"].unique()]
    query = f"SELECT Categoria FROM `{conjunto_datos}.{tabla}` WHERE Categoria IN ({', '.join(categorias)})"

    query_job = bq_client.query(query)
    results = query_job.result()
    categorias_existentes = [row["Categoria"] for row in results]
    registros_leidos = df_categorias.shape[0]
    df_categorias = df_categorias[~df_categorias["Categoria"].isin(categorias_existentes)]


    # Mostrar estadísticas
    print("*--------------------------------------------*")
    print("*                                            *")
    print("*          Estadísticas de Ejecución         *")
    print("*                                            *")
    print("*--------------------------------------------*")
    print("*")
    print(f' Total de registros Leidos       : {registros_leidos}')

    if not df_categorias.empty:
        id_inicial = obtener_proximo_id_categoria(bq_client, conjunto_datos, tabla)
        insertar_categorias_en_bigquery(bq_client, conjunto_datos, tabla, df_categorias, id_inicial)

        # Registra en la tabla de Auditoria después de cargar Continentes
        registros_auditoria = registrar_auditoria_cargar_continente(bq_client, registros_leidos, df_categorias.shape[0])

        # Mostrar estadísticas   
        print(f' Total de registros insertados   : {df_categorias.shape[0]}')
        print("*")
        print(f" Registros de Auditoria Generados: {registros_auditoria}")
        print("*")

        return 'Carga de Incremental en tabla Categorías completada en BigQuerys.'
    else:
        # Registra en la tabla de Auditoria después de cargar Continentes
        registros_auditoria = registrar_auditoria_cargar_continente(bq_client, registros_leidos, df_categorias.shape[0])

        # Mostrar estadísticas   
        print(f' Total de registros insertados   : {0}')
        print("*")
        print(f" Registros de Auditoria Generados: {registros_auditoria}")
        print("*")

        return 'No se requiere carga incremental en tabla Categorías, no hay registros nuevos para insertar.'
