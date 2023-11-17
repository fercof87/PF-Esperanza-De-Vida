import os
from io import StringIO
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery import dbapi
import pandas as pd
import traceback


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

# Definir el esquema de la tabla DatosIndicador
DATOS_INDICADOR_SCHEMA = [
    bigquery.SchemaField('IdDatoIndicador', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('IdPais', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('IdIndicador', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('Valor', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('Año', 'INTEGER', mode='NULLABLE'),
]


def descargar_datos_indicador_bigquery(bq_client, conjunto_datos, tabla, tabla_indicador, cod_indicador_proceso):
    """
    Descarga todos los registros de la tabla DatosIndicador de BigQuery a un DataFrame.

    :param bq_client: Cliente de BigQuery.
    :param conjunto_datos: Nombre del conjunto de datos.
    :param tabla: Nombre de la tabla.
    :return: DataFrame con los datos de la tabla.
    """
    try:
        # Construir la consulta SQL con un JOIN
        query = f"""
            SELECT d.IdDatoIndicador, d.IdPais, p.CodPais, d.IdIndicador, i.CodIndicador, d.Valor, d.Anio
            FROM `{conjunto_datos}.{tabla}` AS d
            JOIN `{conjunto_datos}.{tabla_indicador}` AS i 
                ON d.IdIndicador = i.IdIndicador
            JOIN `{conjunto_datos}.Pais` AS p 
                ON d.IdPais = p.IdPais
            WHERE i.CodIndicador = '{cod_indicador_proceso}'
            ORDER BY d.IdDatoIndicador ASC
        """

        # Ejecutar la consulta y descargar los resultados a un DataFrame
        df = bq_client.query(query).to_dataframe()

        # Convertir tipos de datos para optimizar la memoria
        df['IdDatoIndicador'] = df['IdDatoIndicador'].astype('int64')
        df['IdPais'] = df['IdPais'].astype('int64')
        df['CodPais'] = df['CodPais'].astype('string')
        df['IdIndicador'] = df['IdIndicador'].astype('int64')
        df['CodIndicador'] = df['CodIndicador'].astype('string')
        df['Valor'] = df['Valor'].astype('float64')
        df['Anio'] = df['Anio'].astype('int64')

        # Renombrar la columna 'Anio' a 'Año'
        df.rename(columns={'Anio': 'Año'}, inplace=True)
        
        return df

    except Exception as e:
        error_message = f"Error al descargar datos de la tabla {conjunto_datos}.{tabla}: {e}"
        print(error_message)
        raise Exception(error_message)
    

def obtener_max_id_dato_indicador(bq_client, dataset, tabla):
    """
    Obtiene el máximo IdDatoIndicador de la tabla DatosIndicador en BigQuery.

    :param bq_client: Cliente de BigQuery.
    :param dataset: Nombre del conjunto de datos.
    :param tabla: Nombre de la tabla.
    :return: El máximo IdDatoIndicador.
    """
    try:
        query = f'SELECT MAX(IdDatoIndicador) AS max_id FROM `{dataset}.{tabla}`'
        query_job = bq_client.query(query)
        result = query_job.result()
        row = next(result)
        max_id = row['max_id']

        return max_id if max_id is not None else 0

    except Exception as e:
        raise Exception(f"Error al obtener el máximo IdDatoIndicador: {e}")



def obtener_mapeo_cod_a_id(bq_client, dataset, tabla):
    """
    Obtiene un mapeo de códigos a identificadores desde una tabla en BigQuery.

    :param bq_client: Cliente de BigQuery.
    :param dataset: Nombre del conjunto de datos.
    :param tabla: Nombre de la tabla.
    :return: Diccionario de mapeo de códigos a identificadores.
    """
    query = f"""
        SELECT CodIndicador, IdIndicador
        FROM `{dataset}.{tabla}`
    """
    query_job = bq_client.query(query)
    results = query_job.result()

    mapeo_cod_a_id = {}
    for row in results:
        mapeo_cod_a_id[row['CodIndicador']] = row['IdIndicador']

    return mapeo_cod_a_id


def obtener_mapeo_cod_a_id_pais(bq_client, conjunto_datos):
    """
    Obtiene un mapeo de CodPais a IdPais.

    :param bq_client: Cliente de BigQuery.
    :param conjunto_datos: Nombre del conjunto de datos.
    :return: Diccionario que mapea CodPais a IdPais.
    """
    try:
        # Construir la consulta SQL para obtener el mapeo
        query = f"SELECT CodPais, IdPais FROM `{conjunto_datos}.Pais`"
        
        # Ejecutar la consulta y descargar los resultados a un DataFrame
        df = bq_client.query(query).to_dataframe()

        # Crear un diccionario de mapeo
        mapeo_cod_a_id_pais = dict(zip(df['CodPais'], df['IdPais']))

        return mapeo_cod_a_id_pais

    except Exception as e:
        error_message = f"Error al obtener el mapeo CodPais a IdPais: {e}"
        print(error_message)
        raise Exception(error_message)


def insertar_actualizar_datos_indicador(bq_client, conjunto_datos, tabla, df_datos_csv):
    """
    Inserta y actualiza registros en la tabla DatosIndicador de BigQuery.

    :param bq_client: Cliente de BigQuery.
    :param conjunto_datos: Nombre del conjunto de datos.
    :param tabla: Nombre de la tabla.
    :param df_datos_csv: DataFrame con los datos a insertar o actualizar.
    :param df_datos_bigquery: DataFrame con los datos actuales en la tabla.
    :return: Tupla con la cantidad de registros insertados y la cantidad de registros actualizados.
    """
    try:
        # Verificar que las columnas requeridas estén presentes en df_datos_csv
        required_columns = ['CodPais', 'Año', 'CodIndicador', 'Valor']
        if not all(col in df_datos_csv.columns for col in required_columns):
            raise ValueError(f"Las columnas requeridas {required_columns} no están presentes en df_datos_csv.")

        # Obtener mapeo de CodPais a IdPais y de CodIndicador a IdIndicador
        mapeo_cod_a_id_pais = obtener_mapeo_cod_a_id_pais(bq_client, conjunto_datos)
        mapeo_cod_a_id_indicador = obtener_mapeo_cod_a_id(bq_client, conjunto_datos, 'Indicador')

        # Inicializar estructuras para insertar y actualizar
        registros_a_insertar = []
        registros_a_modificar = 0

        # Obtener el máximo IdDatoIndicador actual
        max_id_dato_indicador = obtener_max_id_dato_indicador(bq_client, conjunto_datos, tabla)

        #Variable usada para fraccionar las descargas de bigquery y reducir los df a utilizar
        cod_indicador_proceso = None

        # Iterar sobre los registros del DataFrame de datos CSV
        for index, row_csv in df_datos_csv.iterrows():

            if (cod_indicador_proceso == None or cod_indicador_proceso != row_csv['CodIndicador']):
                cod_indicador_proceso = row_csv['CodIndicador']
                df_datos_bigquery = descargar_datos_indicador_bigquery(bq_client, conjunto_datos, tabla, 'Indicador', cod_indicador_proceso)

            # Obtener IdPais a partir de CodPais
            id_pais = mapeo_cod_a_id_pais.get(row_csv['CodPais'])
            if id_pais is None:
                raise ValueError(f"No se encontró IdPais para CodPais: {row_csv['CodPais']}")

            # Obtener IdIndicador a partir de CodIndicador
            id_indicador = mapeo_cod_a_id_indicador.get(row_csv['CodIndicador'])
            if id_indicador is None:
                raise ValueError(f"No se encontró IdIndicador para CodIndicador: {row_csv['CodIndicador']}")

            # Filtrar el DataFrame de datos BigQuery para encontrar el registro correspondiente
            filtro = (df_datos_bigquery['CodPais'] == row_csv['CodPais']) & \
                     (df_datos_bigquery['Año'] == row_csv['Año']) & \
                     (df_datos_bigquery['CodIndicador'] == row_csv['CodIndicador'])

            registros_encontrados = df_datos_bigquery[filtro]

            if registros_encontrados.empty:
                # Incrementar el máximo IdDatoIndicador
                max_id_dato_indicador += 1
                # No se encontró el registro, agregar a la lista de insertar
                registros_a_insertar.append({
                    'IdDatoIndicador': max_id_dato_indicador,
                    'IdPais': id_pais,
                    'IdIndicador': id_indicador,
                    'Anio': row_csv['Año'],
                    'Valor': row_csv['Valor']
                })
            else:
                # Se encontró el registro, comparar Valor
                registro_encontrado = registros_encontrados.iloc[0]
                if row_csv['Valor'] != registro_encontrado['Valor']:
                    registros_a_modificar +=1
                    print("registros a modificar = ", registros_a_modificar)
                    # Hay diferencias, realizar la actualización en BigQuery
                    query_update = f"""
                        UPDATE `{conjunto_datos}.{tabla}`
                        SET Valor = {row_csv['Valor']}
                        WHERE CodPais = '{row_csv['CodPais']}' AND Año = {row_csv['Año']} AND CodIndicador = '{row_csv['CodIndicador']}'
                    """
                    bq_client.query(query_update).result()

        # Convertir la lista de registros a insertar a DataFrame
        df_registros_a_insertar = pd.DataFrame(registros_a_insertar)

        # Insertar registros en la tabla DatosIndicador
        table_ref = bq_client.dataset(conjunto_datos).table(tabla)
        table = bq_client.get_table(table_ref)

        # Operación de carga para registros a insertar
        if len(registros_a_insertar):
            errors_insert = bq_client.insert_rows(table, df_registros_a_insertar.to_dict(orient='records'))
        
            # Verificar errores en la carga de registros a insertar
            if errors_insert:
                raise Exception(f"Error al insertar registros en la tabla {conjunto_datos}.{tabla}: {errors_insert}")

        # Retornar la cantidad de registros insertados y actualizados
        return len(registros_a_insertar), registros_a_modificar

    except Exception as e:
        error_message = f"Error al insertar y actualizar registros en la tabla {conjunto_datos}.{tabla}: {e}"
        print(error_message)
        import traceback
        traceback.print_exc()
        raise Exception(error_message)



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
        
        if max_nro == None:
            return 1
        return max_nro + 1

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
    Registra en la tabla de Auditoria después de verificar archivos.

    :param bq_client: Cliente de BigQuery.
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
            'Proceso': 'transformar_imputar_BM',
            'Tabla': 'DatosIndicador',
            'RegLeidos': registros_leidos,
            'RegInsertados': registros_insertados,
            'RegActualizados': registros_modificados,
            'RegEliminados': 0,
        }

        # Convertir el diccionario a un DataFrame
        df = pd.DataFrame([auditoria_data])

        dataset_ref = bq_client.dataset("Modelo_Esperanza_De_Vida")
        table_ref = dataset_ref.table("Auditoria")

        job_config = bigquery.LoadJobConfig(
            schema=AUDITORIA_SCHEMA,
            write_disposition='WRITE_APPEND',
        )

        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

        print("Inserción en tabla Auditoria después de verificar archivos. Registros insertados: ", df.shape[0])
        return df.shape[0]
        
    except Exception as e:
        raise Exception(f"Error al insertar en tabla Auditoria después de verificar archivos: {e}")
    


def leer_archivo_a_df(cliente, bucket_name, blob_name):
    """
    Lee un archivo desde Google Cloud Storage y lo convierte en un DataFrame de Pandas.

    Args:
        cliente (google.cloud.storage.Client): Cliente de almacenamiento.
        bucket_name (str): Nombre del bucket en Google Cloud Storage.
        blob_name (str): Nombre del blob (archivo) en el bucket.

    Returns:
        pd.DataFrame: DataFrame con los datos del archivo.

    Raises:
        Exception: Si el archivo no se encuentra o no puede ser leído.
    """
    try:
        # Verificar la existencia del archivo en el bucket
        bucket = cliente.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if not blob.exists():
            raise Exception(f"El archivo {blob_name} no se encuentra en el bucket {bucket_name}.")

        # Descargar y leer el archivo como texto
        data = blob.download_as_text()

        # Convertir el texto a un DataFrame de Pandas
        return pd.read_csv(StringIO(data))

    except Exception as e:
        error_message = f"Error al leer el archivo {blob_name} desde el bucket {bucket_name}: {e}"
        print(error_message)
        raise Exception(error_message)


def buscar_descripcion_indicador(bq_client, conjunto_datos, tabla, df):
    """
    Busca la descripción de los indicadores en la tabla Indicador en BigQuery.

    :param bq_client: Cliente de BigQuery.
    :param df: DataFrame con la columna CodIndicador.
    :return: DataFrame con los resultados de la búsqueda (CodIndicador, Descripcion).
    """
    try:
        print(df.columns)
        
        # Extraer los códigos de indicadores únicos del DataFrame
        codigos_indicadores = df['CodIndicador'].unique()

        # Convertir los códigos de indicadores a una cadena de texto para la consulta SQL
        codigos_str = ', '.join([f'"{cod}"' for cod in codigos_indicadores])

        # Construir la consulta SQL
        query = f'SELECT CodIndicador, Descripcion FROM `{conjunto_datos}.{tabla}` WHERE CodIndicador IN ({codigos_str})'

        # Ejecutar la consulta y descargar los resultados a un DataFrame
        df_resultado = bq_client.query(query).to_dataframe()

        return df_resultado

    except Exception as e:
        print(f"Error al buscar descripciones de indicadores: {e}")
        raise Exception(f"Error al buscar descripciones de indicadores: {e}")




def transformar_imputar_BM(request):
    """
    Realiza la transformación e imputación de valores sobre los datos descargados del BM.
    Toma el archivo intermedio banco_mundial_data_a_registros.csv, en donde los indicadores
    están a nivel de registro.
    Args:
        request (flask.Request): La solicitud HTTP.
    Returns:
        str: Un mensaje indicando si los archivos están presentes o no.
    Raises:
        Exception: Si hay archivos faltantes, se lanza una excepción con un mensaje detallado.
    """
    try:

        # Nombre del bucket y archivo de input
        bucket_name_intermedios       = "pf-henry-esperanza-archivos-intermedios"
        bucket_name_parametros        = "pf-henry-esperanza-parametros"
        blob_name_datos               = 'banco_mundial_data_a_registros.csv'
        blob_name_indicadores         = 'Parametros_Indicadores.csv'

        # Crear Cliente de storage
        storage_client = storage.Client()
        # Configurar el cliente de BigQuery
        bq_client = bigquery.Client()
        conjunto_datos = 'Modelo_Esperanza_De_Vida'
        tabla = 'DatosIndicador'
        tabla_indicador = 'Indicador'

        # Crear Cliente y Cargar el archivo con los datos del BM desde el bucket a un DataFrame
        df_datos = leer_archivo_a_df(storage_client, bucket_name_intermedios, blob_name_datos)
        # Renombrar la columna 'Anio' a 'Año'
        df_datos.rename(columns={'Indicador': 'CodIndicador'}, inplace=True)

        # Crear Cliente y Cargar el archivo con los indicadores a procesar desde el bucket a un DataFrame
        df_indicadores = leer_archivo_a_df(storage_client, bucket_name_parametros, blob_name_indicadores)
        #Agrego la columna Descripcion por cada Indicador
        df_indicadores = buscar_descripcion_indicador(bq_client, conjunto_datos, tabla_indicador, df_indicadores)

        #Descarga tabla DatosIndicador
        #df_datos_bigquery = descargar_datos_indicador_bigquery(bq_client, conjunto_datos, tabla, tabla_indicador)

        # Totalizadores para estadísticas
        registros_leidos_data        = df_datos.shape[0]
        registros_leidos_indicadores = df_indicadores.shape[0]

        #Proceso de Transformación e imputación de valores
        print(df_indicadores.columns)
        mi_diccionario = df_indicadores.set_index('CodIndicador')['Descripcion'].to_dict()
        df_datos['Indicador_name'] = df_datos['CodIndicador'].map(mi_diccionario)

        df_agg = df_datos.groupby('CodIndicador')['Valor'].count().to_frame().sort_values('Valor', ascending=False)
        df_agg_filtrado = df_agg[df_agg['Valor']> 1000].reset_index()
        indicadores = df_agg_filtrado['CodIndicador']

        df_datos = df_datos[df_datos['CodIndicador'].isin(indicadores)]


        paises = df_datos['CodPais'].unique().tolist()
        indicadores = df_datos['Indicador_name'].unique().tolist()
        anios = df_datos['Año'].unique().tolist()

        df_datos.loc[(df_datos['CodPais'] == "SGP") & (df_datos['Indicador_name'] == "Rural population growth (annual %)"), 'Valor'] = 0

        for pais in paises:
            for indicador in indicadores:
                df_pais_indicador = df_datos[(df_datos['CodPais']==pais) & (df_datos['Indicador_name']==indicador)]
                nulos = df_pais_indicador['Valor'].isna().sum()
                if(nulos > 0.4*(len(anios))):
                    indices = df_pais_indicador.index
                    df_datos.drop(index=indices, inplace=True)
                else:
                    df_datos.loc[(df_datos['CodPais']==pais) & (df_datos['Indicador_name']==indicador) & (df_datos['Valor'].isna()), 'Valor'] = df_pais_indicador['Valor'].mean()

        
        df_datos = df_datos[['CodPais','Año', 'CodIndicador', 'Valor']]

        #descomponer los totales del tratamiento de la tabla DatosIndicador
        registros_insertados, registros_modificados = insertar_actualizar_datos_indicador(bq_client, conjunto_datos, tabla, df_datos)


        # Registra en la tabla de Auditoria después de cargar Continentes
        registros_auditoria = registrar_auditoria(bq_client, registros_leidos_data,registros_insertados,registros_modificados)

        # Mostrar estadísticas
        print("*--------------------------------------------*")
        print("*                                            *")
        print("*          Estadísticas de Ejecución         *")
        print("*                                            *")
        print("*--------------------------------------------*")
        print("*")
        print(f' Total de registros leídos Datos BM      : {registros_leidos_data}')
        print(f' Total de registros leídos Indicadores   : {registros_leidos_indicadores}')
        print("*")
        print(f" Registros Insertados en Datos Indicador : {registros_insertados}")
        print(f" Registros Modificados en Datos Indicador: {registros_modificados}")
        print(f" Registros de Auditoria Generados        : {registros_auditoria}")
        print("*")

        return 'Transformaciones e imputaciones realizadas con éxito en DatosIndicador.'
    
    except Exception as e:
        
        print(f"Error en proceso transformar_imputar_BM: {str(e)}")
        return 'Error en proceso transformar_imputar_BM.'
    