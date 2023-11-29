import pandas as pd
from pandas_gbq import to_gbq
import wbgapi as wb
from google.cloud import bigquery
from google.cloud import storage
import io


#----------------------------------#
#            CONSTANTES            #
#----------------------------------#
# Definir el tamaño máximo de lote
TAMANO_LOTE = 100

#----------------------------------#
#            GLOBALES              #
#----------------------------------#
total_insertados_indicador  = 0
total_modificados_indicador = 0
total_insertadas_relaciones = 0
total_eliminadas_relaciones = 0


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


def registrar_auditoria_cargar_continente(bq_client, tabla, registros_leidos, registros_insertados, registros_modificados, registros_eliminados):
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
            'Proceso': 'cargar_indicadores',
            'Tabla': tabla,
            'RegLeidos': registros_leidos,
            'RegInsertados': registros_insertados,
            'RegActualizados': registros_modificados,
            'RegEliminados': registros_eliminados,
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
        raise Exception(f"Error al insertar en tabla Auditoria: {e}")


def buscar_datos_indicador(cod_indicador):
    """
    Busca la información de un indicador en la API del Banco Mundial.

    :param cod_indicador: Código del indicador a buscar.

    :return: La descripción del indicador o None si no se encuentra.
    """
    try:
        ind_info = wb.series.get(cod_indicador)
        if ind_info:
            return ind_info['value']
    except Exception as e:
        # Manejo de errores en caso de problemas al buscar el indicador.
        print(f"Error al buscar el indicador {cod_indicador}: {str(e)}")
    return None



def buscar_datos_indicadores_en_lote(codigos_indicadores):
    """
    Busca la información de varios indicadores en la API del Banco Mundial.

    :param codigos_indicadores: Lista de códigos de indicadores a buscar.

    :return: Un diccionario con los códigos de indicadores como clave y sus descripciones como valor.
    """
    indicadores_info = {}
    for cod_indicador in codigos_indicadores:
        descripcion = buscar_datos_indicador(cod_indicador)
        indicadores_info[cod_indicador] = descripcion
    return indicadores_info



def insertar_nuevo_indicador_en_lote(bq_client, conjunto_datos, tabla, indicadores):
    """
    Inserta nuevos indicadores en la tabla Indicador en lotes.

    :param bq_client: Cliente de BigQuery.
    :param conjunto_datos: Nombre del conjunto de datos en BigQuery.
    :param tabla: Nombre de la tabla en la que insertar los nuevos indicadores.
    :param indicadores: Lista de diccionarios, cada uno representando un indicador con la siguiente estructura:
                        {'cod_indicador': 'Código del indicador', 'ind_info': 'Descripción del indicador', 'new_id': 'Nuevo IdIndicador'}
    """
    try:
        # Crear un DataFrame con los indicadores
        df_indicadores = pd.DataFrame(indicadores)

        # Insertar el lote de nuevos indicadores en la tabla usando pandas_gbq
        to_gbq(df_indicadores, f'{conjunto_datos}.{tabla}', if_exists='append', project_id=bq_client.project)
    except Exception as e:
        # Manejo de errores en caso de problemas al insertar los indicadores.
        print(f"Error al insertar indicadores: {str(e)}")



def actualizar_descripcion_indicador(bq_client, conjunto_datos, tabla, id_indicador, ind_info):
    """
    Actualiza la descripción de un indicador existente en la tabla Indicador.

    :param bq_client: Cliente de BigQuery.
    :param conjunto_datos: Nombre del conjunto de datos en BigQuery.
    :param tabla: Nombre de la tabla en la que actualizar el indicador.
    :param id_indicador: IdIndicador del indicador a actualizar.
    :param ind_info: Nueva descripción para el indicador.

    :return: True si la actualización se realiza con éxito, False si hay un error.
    """
    try:
        update_query = f'''
            UPDATE `{conjunto_datos}.{tabla}`
            SET Descripcion = "{ind_info}"
            WHERE IdIndicador = {id_indicador}
        '''
        update_query_job = bq_client.query(update_query)
        update_query_job.result()
        return True
    except Exception as e:
        # Manejo de errores en caso de problemas al actualizar la descripción del indicador.
        print(f"Error al actualizar el indicador {id_indicador}: {str(e)}")
        return False



def obtener_categorias_indicador(indicadores_por_categoria, cod_indicador):
    """
    Obtiene las categorías de un indicador a partir de su código.

    :param indicadores_por_categoria: Lista de diccionarios que mapea códigos de indicadores a categorías.
    :param cod_indicador: Código del indicador.

    :return: Una lista de categorías asociadas al indicador o una lista vacía si no se encuentran categorías.
    """
    for indicador in indicadores_por_categoria:
        if indicador['CodIndicador'] == cod_indicador:
            return indicador['IdCategorias']

    # Si no se encuentra el indicador en la lista
    return []



def obtener_categorias_indicadores(codigos_indicadores):
    """
    Obtiene las categorías de un conjunto de indicadores a partir de sus códigos.
    Los datos son obtenidos de la API del Banco Mundial de Datos.

    :param codigos_indicadores: Lista de códigos de indicadores.

    :return: Una lista de diccionarios con los campos CodIndicador e IdCategorias.
    """
    categorias_por_indicador = {}

    for topic_id in range(1, 22):
        try:
            # Obtener todos los códigos de indicadores de esa categoría (topic_id)
            indicators_by_topic = wb.series.info(topic=topic_id)

            # Guardar en un conjunto los códigos de indicador que pertenecen a esa categoría
            lista_ids = {item['id'] for item in indicators_by_topic.items}

            # Actualizar el diccionario con los campos CodIndicador e IdCategorias
            for cod_indicador in lista_ids:
                if cod_indicador in codigos_indicadores:
                    if cod_indicador not in categorias_por_indicador:
                        categorias_por_indicador[cod_indicador] = set()
                    categorias_por_indicador[cod_indicador].add(topic_id)
        except Exception as e:
            # Manejar la excepción y posiblemente registrarla para su análisis
            print(f"Error al obtener categorías para topic_id {topic_id}: {str(e)}")

    # Crear la lista de diccionarios resultante
    #categorias_x_CodIndicador = [{'CodIndicador': cod_indicador, 'IdCategorias': list(id_categorias)}
    #             for cod_indicador, id_categorias in categorias_por_indicador.items()]
    categorias_x_CodIndicador = [{'CodIndicador': cod_indicador, 'NroTopicoBM': list(id_categorias)}
                 for cod_indicador, id_categorias in categorias_por_indicador.items()]
    return categorias_x_CodIndicador




def buscar_id_categoria(bq_client, conjunto_datos, categoria):
    """
    Busca el IdCategoria en la tabla 'Categoria' a partir de la descripción de la categoría.

    :param bq_client: Cliente de BigQuery.
    :param conjunto_datos: Nombre del conjunto de datos en BigQuery.
    :param categoria: Descripción de la categoría a buscar.

    :return: El IdCategoria correspondiente a la descripción de la categoría o None si no se encuentra.
    """
    try:
        query = f'SELECT IdCategoria FROM `{conjunto_datos}.Categoria` WHERE Categoria = {categoria!r}'
        query_job = bq_client.query(query)
        resultados = list(query_job.result())

        if resultados:
            return resultados[0]['IdCategoria']
        else:
            return None
    except Exception as e:
        # Manejo de errores en caso de problemas al buscar el IdCategoria.
        print(f"Error al buscar el IdCategoria para la categoría {categoria}: {str(e)}")
        return None




def obtener_max_id(bq_client, conjunto_datos, tabla):
    """
    Obtiene el valor máximo actual de IdIndicador en una tabla específica.

    :param bq_client: Cliente de BigQuery.
    :param conjunto_datos: Nombre del conjunto de datos en BigQuery.
    :param tabla: Nombre de la tabla en la que buscar el valor máximo de IdIndicador.

    :return: El valor máximo actual de IdIndicador más 1. Si no hay resultados, devuelve 0.
    """
    max_id_query = f'SELECT MAX(IdIndicador) AS max_id FROM `{conjunto_datos}.{tabla}`'
    max_id_query_job = bq_client.query(max_id_query)
    max_id_result = list(max_id_query_job.result())
    max_id = 0 if not max_id_result or max_id_result[0]['max_id'] is None else (max_id_result[0]['max_id'] + 1)
    return max_id





def insertar_nuevas_categorias_en_lote(bq_client, conjunto_datos, tabla, categorias):
    """
    Inserta nuevas categorías en la tabla IndicadorCategoria en lotes.

    :param bq_client: Cliente de BigQuery.
    :param conjunto_datos: Nombre del conjunto de datos en BigQuery.
    :param tabla: Nombre de la tabla en la que insertar las nuevas categorías.
    :param categorias: Lista de diccionarios, cada uno representando una categoría con la siguiente estructura:
                       {'IdIndicador': 'IdIndicador asociado a la categoría', 'IdCategoria': 'IdCategoria a insertar'}
    """
    try:
        # Crear un DataFrame con las categorías
        df_categorias = pd.DataFrame(categorias)

        # Insertar el lote de nuevas categorías en la tabla usando pandas_gbq
        to_gbq(df_categorias, f'{conjunto_datos}.{tabla}', if_exists='append', project_id=bq_client.project)
    except Exception as e:
        # Manejo de errores en caso de problemas al insertar las categorías.
        print(f"Error al insertar categorías: {str(e)}")



def eliminar_categorias_en_lote(bq_client, conjunto_datos, tabla, categorias):
    """
    Elimina categorías de la tabla IndicadorCategoria en lotes.

    :param bq_client: Cliente de BigQuery.
    :param conjunto_datos: Nombre del conjunto de datos en BigQuery.
    :param tabla: Nombre de la tabla de la que eliminar las categorías.
    :param categorias: Lista de diccionarios, cada uno representando una categoría con la siguiente estructura:
                       {'IdIndicador': 'IdIndicador asociado a la categoría', 'IdCategoria': 'IdCategoria a eliminar'}
    """
    try:
        # Crear un DataFrame con las categorías
        df_categorias = pd.DataFrame(categorias)

        # Eliminar el lote de categorías de la tabla usando pandas_gbq
        to_gbq(df_categorias, f'{conjunto_datos}.{tabla}', if_exists='replace', project_id=bq_client.project)
    except Exception as e:
        # Manejo de errores en caso de problemas al eliminar las categorías.
        print(f"Error al eliminar categorías: {str(e)}")



def obtener_tabla_categoria(client, dataset_id, table_id):
    """
    Descarga la tabla Categoria desde BigQuery y retorna un DataFrame.

    :param dataset_id: ID del conjunto de datos.
    :param table_id: ID de la tabla.
    :return: Un DataFrame con la tabla Categoria.
    """
    try:

        # Consulta SQL para seleccionar las columnas específicas de la tabla Indicador
        query = f"SELECT IdCategoria, Categoria, NroTopicoBM FROM `{dataset_id}.{table_id}`"

        # Ejecutar la consulta
        query_job = client.query(query)

        # Obtener los resultados en un DataFrame de pandas
        df = query_job.to_dataframe()

        return df

    except Exception as e:
        # Manejar el error y posiblemente registrar información detallada
        print(f"Error al obtener la tabla Indicador: {str(e)}")
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error        


def generar_categorias_x_indicador(df_indicadores_bigquery, categorias_x_indicador):
    """
    Genera una lista de diccionarios que mapea los códigos de indicador a las categorías correspondientes.

    :param df_indicadores_bigquery: DataFrame con la información de los indicadores.
    :param categorias_x_indicador: Lista de diccionarios con los códigos de indicador y sus respectivos NroTopicoBM.
    :return: lista con diccionarios con las claves CodIndicador e IdCategoria.
    """

    resultados_list = []

    # Validar que las columnas requeridas estén presentes en df_indicadores_bigquery
    required_columns = ['IdCategoria', 'Categoria', 'NroTopicoBM']
    for col in required_columns:
        if col not in df_indicadores_bigquery.columns:
            raise ValueError(f'La columna {col} no está presente en df_indicadores_bigquery.')

    # Iterar sobre cada entrada en categorias_x_indicador
    for entry in categorias_x_indicador:
        cod_indicador = entry.get('CodIndicador')
        
        if cod_indicador is None:
            print('Advertencia: Se encontró una entrada en categorias_x_indicador sin CodIndicador.')
            continue

        nro_topicos = entry.get('NroTopicoBM', [])

        # Filtrar df_indicadores_bigquery por CodIndicador y NroTopicoBM
        filtro = (df_indicadores_bigquery['NroTopicoBM'].isin(nro_topicos))

        # Verificar si se encontraron resultados
        if df_indicadores_bigquery.loc[filtro].empty:
            print(f'Advertencia: No se encontraron resultados para CodIndicador {cod_indicador} y NroTopicoBM {nro_topicos}.')
            continue

        # Obtener los IdCategorias correspondientes
        id_categorias = df_indicadores_bigquery.loc[filtro, 'IdCategoria'].tolist()

        # Crear diccionario para la entrada actual
        resultado_entry = {'CodIndicador': cod_indicador, 'IdCategorias': id_categorias}

        # Agregar a la lista de resultados
        resultados_list.append(resultado_entry)

    return resultados_list


def procesar_lote(df_lote, bq_client, conjunto_datos, tabla_indicador, tabla_indicador_categoria, max_id):
    """
    Procesa un lote de datos para actualizar la tabla de indicadores y sus relaciones de categoría en BigQuery.

    :param df_lote: DataFrame que contiene el lote de datos a procesar.
    :param bq_client: Cliente de BigQuery.
    :param conjunto_datos: Nombre del conjunto de datos en BigQuery.
    :param tabla_indicador: Nombre de la tabla de indicadores en BigQuery.
    :param tabla_indicador_categoria: Nombre de la tabla de relaciones de categoría en BigQuery.

    :return: None
    """
    global total_insertados_indicador
    global total_modificados_indicador
    global total_insertadas_relaciones
    global total_eliminadas_relaciones


    nuevos_indicadores = []
    categorias_a_agregar = []
    categorias_a_eliminar = []

    # Obtener la lista de códigos de indicadores en el lote
    codigos_indicadores_lote = df_lote['CodIndicador'].tolist()

    #------------------------------------------------------------------------------------------------------------------------*
    #                                 TRATAMIENTO TABLA Indicador                                                            *
    #------------------------------------------------------------------------------------------------------------------------*
    for _, row in df_lote.iterrows():
        cod_indicador = row['CodIndicador']
        #Buscamos la Descripción de cada CodIndicador
        ind_info = buscar_datos_indicador(cod_indicador)

        # Verificar si el indicador ya existe en la tabla Indicador buscando por CodIndicador
        query = f'''
            SELECT IdIndicador, Descripcion
            FROM `{conjunto_datos}.{tabla_indicador}`
            WHERE CodIndicador = "{cod_indicador}"
        '''
        query_job = bq_client.query(query)
        results = list(query_job.result())

        if results:
            # Si el indicador existe en la tabla, verifico si cambio la Descripción retornada
            # por el banco mundial, en ese caso realizo un UPDATE en la tabla para modificarla.
            existing_id, existing_description = results[0]['IdIndicador'], results[0]['Descripcion']
            if existing_description != ind_info:
                print(f"Actualizando descripción para IdIndicador {existing_id}")
                # La descripción ha cambiado, actualizamos la tabla Indicador
                if actualizar_descripcion_indicador(bq_client, conjunto_datos, tabla_indicador, existing_id, ind_info):
                    total_modificados_indicador += 1
        else:
            # Si no encontramos el CodIndicador en la tabla, se realiza un insert del mismo para
            # agregarlo al modelo de bigquery.
            # Incrementar el valor máximo para el próximo indicador
            max_id += 1
            # El indicador no existe, lo agregamos a la lista de nuevos indicadores
            nuevos_indicadores.append({'IdIndicador': max_id, 'CodIndicador': cod_indicador, 'Descripcion': ind_info})
            total_insertados_indicador += 1

    # Insertar todos los nuevos indicadores en una operación
    # Paso la LISTA de indicadores a la función para insertar todo el lote de indicadores.
    if nuevos_indicadores:
        insertar_nuevo_indicador_en_lote(bq_client, conjunto_datos, tabla_indicador, nuevos_indicadores)


    # Genero una lista de indicadores para pasar a la query en formato str
    codigos_indicadores_str = ', '.join([f'"{cod}"' for cod in codigos_indicadores_lote])

    # Obtener los IdIndicador para todos los CodIndicador en el lote, en este punto 
    # todos los indicadores del lote están presentes en la tabla Indicador
    id_indicadores_query = f'''
        SELECT IdIndicador, CodIndicador
        FROM `{conjunto_datos}.{tabla_indicador}`
        WHERE CodIndicador IN ({codigos_indicadores_str})
        ORDER BY IdIndicador ASC
    '''

    query_params = [bigquery.ArrayQueryParameter("codigos_indicadores_lote", "STRING", codigos_indicadores_lote)]
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    query_job = bq_client.query(id_indicadores_query, job_config=job_config)
    id_indicadores_result = {row['CodIndicador']: row['IdIndicador'] for row in query_job.result()}

    #------------------------------------------------------------------------------------------------------------------------*
    #                                 TRATAMIENTO TABLA IndicadorCategoria                                                   *
    #------------------------------------------------------------------------------------------------------------------------*

    # Consultar las categorías presentes en tabla IndicadorCategoria (relaciones) para los IdIndicador obtenidos
    id_indicadores_str = ', '.join([str(id_indicador) for id_indicador in id_indicadores_result.values()])
    categorias_por_indicador_query = f'''
        SELECT IdIndicador, IdCategoria
        FROM `{conjunto_datos}.{tabla_indicador_categoria}`
        WHERE IdIndicador IN ({id_indicadores_str})
        ORDER BY IdIndicador ASC, IdCategoria ASC
    '''

    query_params = [bigquery.ArrayQueryParameter("id_indicadores_result", "INT64", list(id_indicadores_result.values()))]
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    query_job = bq_client.query(categorias_por_indicador_query, job_config=job_config)
    

    categorias_por_indicador = {}
    # Recupero las categorias cargadas en IndicadorCategoria para todos los IdIndicador de tabla Indicador
    for row in query_job.result():
        id_indicador = row['IdIndicador']
        id_categoria = row['IdCategoria']
        
        if id_indicador not in categorias_por_indicador:
            categorias_por_indicador[id_indicador] = [id_categoria]
        else:
            categorias_por_indicador[id_indicador].append(id_categoria)

    # Recupero los Nros de Tópicos de cada CodIndicador del Lote Completo
    # Ejemplo del registro que retorna la función:
    # [{'CodIndicador': 'FP.CPI.TOTL.ZG', 'NroTopicoBM': [3, 7]},...]
    categorias_x_indicador  = obtener_categorias_indicadores(codigos_indicadores_lote)

    # Necesito convertir los Nros de Tópicos presentes en categorias_x_indicador a IdCategoria
    df_indicadores_bigquery = obtener_tabla_categoria(bq_client, conjunto_datos, 'Categoria')
    categorias_x_indicador  = generar_categorias_x_indicador(df_indicadores_bigquery, categorias_x_indicador)



    for _, row in df_lote.iterrows():
        cod_indicador = row['CodIndicador']

        # Obtener IdIndicador para el cod_indicador
        id_indicador = id_indicadores_result.get(cod_indicador)

        if id_indicador is not None:
            
            # Obtener las categorías actuales para el IdIndicador
            existing_categories_raw = categorias_por_indicador.get(id_indicador, [])
            existing_categories = set(existing_categories_raw) if isinstance(existing_categories_raw, (list, set)) else {existing_categories_raw}


            # Obtener las nuevas categorías para el cod_indicador
            new_categories = obtener_categorias_indicador(categorias_x_indicador, cod_indicador)


            # Calcular las categorías a agregar y eliminar
            categories_to_add = set(new_categories) - existing_categories
            categories_to_remove = existing_categories - set(new_categories)

            # Insertar nuevas relaciones en IndicadorCategoria
            for category in categories_to_add:
                categorias_a_agregar.append({'IdIndicador': id_indicador, 'IdCategoria': category})
                total_insertadas_relaciones += 1

            # Eliminar relaciones en IndicadorCategoria
            for category in categories_to_remove:
                categorias_a_eliminar.append({'IdIndicador': id_indicador, 'IdCategoria': category})
                total_eliminadas_relaciones += 1

    # Insertar todas las nuevas categorías en bloque
    if categorias_a_agregar:
        insertar_nuevas_categorias_en_lote(bq_client, conjunto_datos, tabla_indicador_categoria, categorias_a_agregar)

    # Eliminar todas las categorías en bloque
    if categorias_a_eliminar:
        eliminar_categorias_en_lote(bq_client, conjunto_datos, tabla_indicador_categoria, categorias_a_eliminar)



def cargar_indicadores(request):
    """
    Carga indicadores desde un archivo CSV en un bucket de Google Cloud Storage a BigQuery.

    :param request: Objeto de solicitud de la función.

    :return: Mensaje indicando que el proceso de carga de indicadores ha sido completado.
    """
    global total_insertados_indicador
    global total_modificados_indicador
    global total_insertadas_relaciones
    global total_eliminadas_relaciones
    cantidad_lotes = 0

    # Configuración de BigQuery
    bq_client = bigquery.Client()
    conjunto_datos = 'Modelo_Esperanza_De_Vida'
    tabla_indicador = 'Indicador'
    tabla_indicador_categoria = 'IndicadorCategoria'

    # Configuración de Cloud Storage
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

    # Obtener el valor máximo actual de IdIndicador en tabla Indicador
    max_id = obtener_max_id(bq_client, conjunto_datos, tabla_indicador)  
    
    # Dividir los datos en lotes de tamaño TAMANO_LOTE
    registros_leidos = df_indicadores.shape[0]
    lotes = [df_indicadores[i:i+TAMANO_LOTE] for i in range(0, len(df_indicadores), TAMANO_LOTE)]

    # Procesar lotes secuencialmente
    for lote in lotes:
        cantidad_lotes +=1
        procesar_lote(lote, bq_client, conjunto_datos, tabla_indicador, tabla_indicador_categoria, max_id)

    # Registra en la tabla de Auditoria después de cargar Continentes
    registros_auditoria = 0
    registros_auditoria += registrar_auditoria_cargar_continente(bq_client, 'Indicador', registros_leidos, total_insertados_indicador, total_modificados_indicador, 0)
    registros_auditoria += registrar_auditoria_cargar_continente(bq_client, 'IndicadorCategoria', registros_leidos, total_insertadas_relaciones, 0, total_eliminadas_relaciones)

    # Mostrar estadísticas
    print("*--------------------------------------------*")
    print("*                                            *")
    print("*          Estadísticas de Ejecución         *")
    print("*                                            *")
    print("*--------------------------------------------*")
    print("*")
    print(f"Total de lotes procesados                                   : {cantidad_lotes}")
    print(f"Total general de indicadores insertados en Indicador        : {total_insertados_indicador}")
    print(f"Total general de indicadores modificados en Indicador       : {total_modificados_indicador}")
    print("*")
    print(f"Total general de Relaciones insertadas en IndicadorCategoria: {total_insertadas_relaciones}")
    print(f"Total general de Relaciones eliminadas en IndicadorCategoria: {total_eliminadas_relaciones}")
    print("*")
    print(f"Registros de Auditoria Generados: {registros_auditoria}")
    print("*")
    
    return 'Proceso de carga de Indicador e IndicadorCategoria completado.'
