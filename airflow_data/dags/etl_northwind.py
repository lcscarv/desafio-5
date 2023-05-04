from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import sqlite3
import csv
import pandas as pd

csv_path = 'airflow_data/data/output_orders.csv'
def bd_connect(my_path):
    #Conexão com banco de dados local
    conn = sqlite3.connect('airflow_data/data/Northwind_small.sqlite')
    curr = conn.cursor()
    #Execução das queries utilizando Pandas para geração de arquivo csv.
    order_df = pd.read_sql_query('SELECT * FROM "ORDER"',conn)
    conn.close()
    #Geração de arquivo csv.
    order_df.to_csv(my_path)
    print('CSV File created')
    
def bd_connect_dag_2(my_path):
    #Conexão
    conn = sqlite3.connect('airflow_data/data/Northwind_small.sqlite')
    curr = conn.cursor()
    
    #Execução da query armazenada como Dataframe Pandas
    order_detail_df = pd.read_sql_query('SELECT * FROM OrderDetail',conn)
    conn.close()
    
    #Leitura do arquivo csv gerado pela task anterior
    order_df = pd.read_csv(my_path)
    
    #Operação de Join utilizando Pandas por meio da função merge (inner Join)
    orders_info = order_df.merge(order_detail_df,left_on='Id', right_on='OrderId', how='inner')
    
    #Cálculo da quantidade de pedidos feitos na cidade do Rio de Janeiro
    qtd = orders_info[orders_info['ShipCity'] == 'Rio de Janeiro']['Quantity'].sum()
    
    #Conversão do resultado para str.
    my_sum = str(qtd)
    #Resultado em arquivo csv.
    with open("airflow_data/data/count.txt",'w',newline='') as txt_file:
        txt_file.write(my_sum)
        
        
def export_final_answer():
    import base64

    # Import count
    with open('airflow_data/data/count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("airflow_data/data/final_output.txt","w") as f:
        f.write(base64_message)
    return None

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'NorthwindELT',
    default_args=default_args,
    description='A ELT dag for the Northwind ECommerceData',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 3),
    catchup=False,
    tags=['etl','extration'],
) as dag:
    dag.doc_md = """
        ELT no banco de dados de ecommerce Northwind,
        começando em 2023-05-03. Data atual {{ds}}. 
    """
    
    extract_northwind_task_order = PythonOperator(
         task_id = 'northwind_extract', python_callable = bd_connect,
         op_kwargs = {'my_path':csv_path}

     ) 
    
    extract_northwind_task_orderDetail = PythonOperator(
        task_id ='northwind_extract_ord_det',
        python_callable = bd_connect_dag_2,
        op_kwargs = {'my_path':csv_path}   
    )
    
    
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )
    extract_northwind_task_order >> extract_northwind_task_orderDetail >> export_final_output