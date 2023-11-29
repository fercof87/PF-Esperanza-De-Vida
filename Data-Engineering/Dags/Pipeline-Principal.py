"""
Nombre del DAG: 
    Pipeline-ETL-Google-Functions

Descripción:
    Este DAG orquesta un pipeline de ETL (Extract, Transform, Load) utilizando Apache Airflow.
    El pipeline consta de las siguientes tareas ejecutadas como Google Cloud Functions:
    - Verificar archivos y disparar tareas paralelas.
    - Cargar continente
    - Cargar categorías
    - Extraer datos del Banco Mundial
    - Cargar indicador de rentabilidad
    - Extraer países después de cargar el continente.
    - Cargar países después de extraer los países.
    - Cargar indicadores después de cargar las categorías.
    - Transformar columnas a registros después de extraer datos del Banco Mundial.
    - Transformar e imputar después de la transformación de columnas a registros.
    - Respaldar archivos después de completar todas las tareas anteriores.
    - Mostrar estadísticas después de respaldar archivos.

Este DAG se encarga de programar y ejecutar estas tareas en el orden especificado. Cada tarea utiliza
Google Cloud Functions para llevar a cabo sus operaciones. El DAG se programa para ejecutarse en un
horario específico o según un desencadenador (trigger) definido.

Este DAG es parte integral de un pipeline de datos más amplio que incluye la orquestación de tareas
de extracción, transformación y carga de datos en BigQuery. Los datos se utilizan para generar
informes y paneles en Looker.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


# Define los argumentos predeterminados del DAG
default_args = {
    'owner': 'Fixing-Data',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Crea una instancia de DAG
dag = DAG(
    'Pipeline_Principal',
    default_args=default_args,
    schedule_interval=None,  # Programa el DAG para que se ejecute semanalmente timedelta(days=7)
    catchup=False,  # Evita la ejecución retroactiva de tareas
    description='DAG para orquestar el pipeline ETL con Google Cloud Functions, proyecto Esperanza de Vida.',
    is_paused_upon_creation=True,  # Pausa el DAG al ser creado
)

#----------------#
# Tareas del DAG #
#----------------#

verificar_archivos = BashOperator(
    task_id='verificar_archivos',
    bash_command='gcloud functions call verificar_archivos',
    dag=dag,
)

cargar_continente = BashOperator(
    task_id='cargar_continente',
    bash_command='gcloud functions call cargar_continente',
    dag=dag,
)

cargar_categorias = BashOperator(
    task_id='cargar_categorias',
    bash_command='gcloud functions call cargar_categorias',
    dag=dag,
)

extraer_datos_BM = BashOperator(
    task_id='extraer_datos_BM',
    bash_command='gcloud functions call extraer_datos_BM',
    dag=dag,
)

cargar_indicador_rentabilidad = BashOperator(
    task_id='cargar_indicador_rentabilidad',
    bash_command='gcloud functions call cargar_indicador_rentabilidad',
    dag=dag,
)

extraer_paises = BashOperator(
    task_id='extraer_paises',
    bash_command='gcloud functions call extraer_paises',
    dag=dag,
)

cargar_indicadores = BashOperator(
    task_id='cargar_indicadores',
    bash_command='gcloud functions call cargar_indicadores',
    dag=dag,
)

transformar_columnas_a_registros_BM = BashOperator(
    task_id='transformar_columnas_a_registros_BM',
    bash_command='gcloud functions call transformar_columnas_a_registros_BM',
    dag=dag,
)

cargar_paises = BashOperator(
    task_id='cargar_paises',
    bash_command='gcloud functions call cargar_paises',
    dag=dag,
)


transformar_imputar_BM = BashOperator(
    task_id='transformar_imputar_BM',
    bash_command='gcloud functions call transformar_imputar_BM',
    dag=dag,
)

generar_data_ML = BashOperator(
    task_id='generar_data_ML',
    bash_command='gcloud functions call generar_data_ML',
    dag=dag,
)

clusterizar_paises = BashOperator(
    task_id='clusterizar_paises',
    bash_command='gcloud functions call clusterizar_paises',
    dag=dag,
)

imputar_rentabilidad = BashOperator(
    task_id='imputar_rentabilidad',
    bash_command='gcloud functions call imputar_rentabilidad',
    dag=dag,
)

respaldar_archivos = BashOperator(
    task_id='respaldar_archivos',
    bash_command='gcloud functions call respaldar_archivos',
    dag=dag,
)

mostrar_estadisticas = BashOperator(
    task_id='mostrar_estadisticas',
    bash_command='gcloud functions call mostrar_estadisticas',
    dag=dag,
)

# Secuencia de tareas
verificar_archivos >> [cargar_continente, cargar_categorias, extraer_datos_BM, cargar_indicador_rentabilidad]
cargar_continente >> extraer_paises >> cargar_paises
cargar_categorias >> cargar_indicadores
extraer_datos_BM >> transformar_columnas_a_registros_BM
[transformar_columnas_a_registros_BM, cargar_indicadores, cargar_paises, cargar_indicador_rentabilidad] >> transformar_imputar_BM >> generar_data_ML >> clusterizar_paises >> imputar_rentabilidad >> [respaldar_archivos, mostrar_estadisticas]
