import os
import io
import pandas as pd
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


def registrar_auditoria_cargar_continente(bq_client, registros_leidos, registros_insertados, registros_modificados):
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
            'Proceso': 'cargar_pais',
            'Tabla': 'Pais',
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


def obtener_maximo_id_pais(bq_client, conjunto_datos, tabla):
    try:
        query = f'''
            SELECT MAX(IdPais) as max_id
            FROM `{conjunto_datos}.{tabla}`
        '''
        query_job = bq_client.query(query)
        result = query_job.result()

        max_id = 0
        for row in result:
            max_id = row.max_id

        return max_id if max_id is not None else 0
        
    except Exception as e:
        raise Exception(f"Error al obtener el máximo IdPais: {str(e)}")

def obtener_id_indicador_rentabilidad(bq_client, conjunto_datos, tabla):
    try:
        query = f'''
            SELECT IdIndicadorRentabilidad
            FROM `{conjunto_datos}.{tabla}`
            WHERE Descripcion = "No Imputado"
        '''
        query_job = bq_client.query(query)
        result = query_job.result()

        for row in result:
            return row.IdIndicadorRentabilidad

        return None
    except Exception as e:
        raise Exception(f"Error al obtener el IdIndicadorRentabilidad: {str(e)}")

def cargar_paises(request):
    try:
        bq_client = bigquery.Client()
        conjunto_datos = 'Modelo_Esperanza_De_Vida'
        tabla_paises = 'Pais'
        tabla_indicador_rentabilidad = 'IndicadorRentabilidad'
        bucket_name = 'pf-henry-esperanza-archivos-intermedios'
        blob_name = 'Paises.csv'

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if not blob.exists():
            raise Exception(f"El archivo '{blob_name}' no existe en el bucket '{bucket_name}'.")

        file_content = blob.download_as_text()
        df_countries = pd.read_csv(io.StringIO(file_content))

        registros_insert = []
        registros_update = []

        # Inicializar contadores
        total_registros_insertados = 0
        total_registros_modificados = 0
        total_registros_leidos = len(df_countries)


        for _, row in df_countries.iterrows():
            query = f'''
                SELECT *
                FROM `{conjunto_datos}.{tabla_paises}`
                WHERE CodPais = "{row['CodPais']}"
            '''
            query_job = bq_client.query(query)
            result = query_job.result()


            if result.total_rows == 0:
                total_registros_insertados += 1
                registros_insert.append(row)
            else:
                for r in result:
                    if (
                        r['IdContinente'] != row['IdContinente'] or
                        r['Pais'] != row['Pais'] or
                        r['Capital'] != row['Capital'] or
                        r['Region'] != row['Region'] or
                        r['Latitud'] != row['Latitud'] or
                        r['Longitud'] != row['Longitud']
                    ):
                        total_registros_modificados += 1
                        registros_update.append(row)

        if registros_insert:

            #Buscamos el maximo de IdPais en tabla Pais
            max_id_pais = obtener_maximo_id_pais(bq_client, conjunto_datos, tabla_paises)

            # Agregar la columna IdPais solo si hay registros para insertar
            for i, row in enumerate(registros_insert):
                row['IdPais'] = max_id_pais + i + 1

            # Imputar valor en IdIndicadorRentabilidad
            id_indicador_no_imputado = obtener_id_indicador_rentabilidad(bq_client, conjunto_datos, tabla_indicador_rentabilidad)
            if id_indicador_no_imputado is None:
                raise Exception("No se encontró el IdIndicadorRentabilidad para 'No Imputado'.")
            for row in registros_insert:
                row['IdIndicadorRentabilidad'] = id_indicador_no_imputado

            # Reordenar columnas
            registros_insert = [row[['IdPais', 'IdContinente', 'IdIndicadorRentabilidad', 'CodPais', 'Pais', 'Capital', 'Region', 'Latitud', 'Longitud']] for row in registros_insert]

            schema_paises = [
                bigquery.SchemaField('IdPais', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('IdContinente', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('IdIndicadorRentabilidad', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('CodPais', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('Pais', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('Capital', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('Region', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('Latitud', 'FLOAT', mode='REQUIRED'),
                bigquery.SchemaField('Longitud', 'FLOAT', mode='REQUIRED'),
            ]

            # Crear DataFrame a partir de registros_insert
            registros_insert_df = pd.DataFrame(registros_insert)

            dataset_ref = bq_client.dataset(conjunto_datos)
            table_ref_paises = dataset_ref.table(tabla_paises)

            job_config_paises = bigquery.LoadJobConfig(
                schema=schema_paises,
                autodetect=False,
            )

            job_paises = bq_client.load_table_from_dataframe(
                registros_insert_df,
                table_ref_paises,
                job_config=job_config_paises
            )
            job_paises.result()

        if registros_update:
            for row in registros_update:
                # Crear la sentencia UPDATE
                update_query = f'''
                    UPDATE `{conjunto_datos}.{tabla_paises}`
                    SET
                        IdContinente = {row['IdContinente']},
                        CodPais = "{row['CodPais']}",
                        Pais = "{row['Pais']}",
                        Region = "{row['Region']}",
                        Latitud = {row['Latitud']},
                        Longitud = {row['Longitud']},
                        Capital = "{row['Capital']}"
                    WHERE CodPais = "{row['CodPais']}"
                '''

                # Ejecutar la sentencia UPDATE
                update_job = bq_client.query(update_query)
                update_job.result()

        # Registra en la tabla de Auditoria después de cargar Continentes
        registros_auditoria = registrar_auditoria_cargar_continente(bq_client, total_registros_leidos, total_registros_insertados, total_registros_modificados)

        # Mostrar estadísticas
        print("*--------------------------------------------*")
        print("*                                            *")
        print("*          Estadísticas de Ejecución         *")
        print("*                                            *")
        print("*--------------------------------------------*")
        print("*")
        print(f'Total de registros leídos      : {total_registros_leidos}, ')
        print(f'Total de registros insertados  : {total_registros_insertados}, ')
        print(f'Total de registros modificados : {total_registros_modificados}.')
        print("*")
        print(f'Total de registros de Auditoria: {registros_auditoria}.')
        print("*")
        return ('Carga completada de Countries CSV en BigQuery. ')

    except Exception as e:
        print(f"Error al cargar en BigQuery: {str(e)}")
        return 'Error al cargar en BigQuery.'
