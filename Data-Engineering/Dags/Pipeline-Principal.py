"""
Nombre del DAG: Pipeline-Principal

Descripción:
Este DAG orquesta un pipeline de ETL (Extract, Transform, Load) utilizando Apache Airflow.
El pipeline consta de las siguientes tareas:
1. Renombrar archivos en un bucket de Google Cloud Storage.
2. Extraer datos de la API del Banco Mundial y cargarlos en un bucket.
3. Generar un archivo CSV con categorías de rentabilidad y cargarlo en un bucket.

El DAG se encarga de programar y ejecutar estas tareas en el orden especificado. Cada tarea utiliza
Google Cloud Functions para llevar a cabo sus operaciones. El DAG se programa para ejecutarse en un
horario específico o según un desencadenador (trigger) definido.

Este DAG forma parte de un pipeline de datos más amplio que incluye la orquestación de tareas
de extracción, transformación y carga de datos en BigQuery. Los datos se utilizan para generar
informes y paneles en Looker.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Define los argumentos predeterminados del DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Crea una instancia de DAG
dag = DAG(
    'mi_pipeline_etl',
    default_args=default_args,
    schedule_interval=timedelta(days=7),  # Programa el DAG para que se ejecute semanalmente
    catchup=False,  # Evita la ejecución retroactiva de tareas
    description='Un DAG para orquestar el pipeline ETL',
)

# Tareas del DAG

# Esta tarea renombra archivos (debe implementarse en Cloud Functions o similar)
rename_files = BashOperator(
    task_id='rename_files',
    bash_command='gcloud functions call rename_files',  # Reemplaza con el comando real para invocar tu Cloud Function
    dag=dag,
    docstring='Renombra archivos en Cloud Functions',
)

# Otras tareas de extracción y procesamiento de datos
extract_countries = BashOperator(
    task_id='extract_countries',
    bash_command='gcloud functions call extract_countries',  # Comando para invocar tu Cloud Function correspondiente
    dag=dag,
    docstring='Extrae datos de países en Cloud Functions',
)

extract_banco_mundial = BashOperator(
    task_id='extract_banco_mundial',
    bash_command='gcloud functions call extract_banco_mundial',  # Comando para tu función de Banco Mundial
    dag=dag,
    docstring='Extrae datos del Banco Mundial en Cloud Functions',
)

categorias_rentabilidad = BashOperator(
    task_id='categorias_rentabilidad',
    bash_command='gcloud functions call generar_categorias_rentabilidad',  # Comando para tu función de rentabilidad
    dag=dag,
    docstring='Genera categorías de rentabilidad en Cloud Functions',
)

# Define el orden de ejecución de las tareas
rename_files >> [extract_countries, extract_banco_mundial, categorias_rentabilidad]

# El siguiente bloque permite ejecutar este archivo como un script independiente
# para interactuar con el DAG a través de la interfaz de línea de comandos de Apache Airflow.
# En producción, el DAG se programa automáticamente según la programación especificada.
if __name__ == "__main__":
    dag.cli()
