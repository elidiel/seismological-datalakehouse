from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# --- Configurações básicas da DAG ---
default_args = {
    'owner': 'elidiel',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'retries': 1,
}

with DAG(
    'ingestao_dados_sismologicos',
    default_args=default_args,
    description='Ingestão automática de dados brutos sismológicos no HDFS',
    schedule_interval=None,  # você pode trocar para '0 0 * * *' se quiser rodar todo dia
    catchup=False,
) as dag:

    # --- Tarefa: Executar o script Python de ingestão ---
    executar_ingestao = BashOperator(
        task_id='executar_ingestao_brutos',
        bash_command='python3 /home/elidiel/airflow/dags/ingestao_brutos_completa.py'
    )

    executar_ingestao
