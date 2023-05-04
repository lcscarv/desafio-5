# Desafio 5 - ETL de Banco de dados Northwind usando Airflow.

Esta é uma DAG que tem como objetivo extrair dados do banco de dados Northwind E-commerce e realizar algumas operações ELT.
Foi desenvolvido em Linux e ambiente virtual de Python. 

# Tarefas
* northwind_extract:

  Extrai dados da tabela Order do banco de dados Northwind e cria um arquivo CSV com essas informações.

* northwind_extract_ord_det:

  Executa uma consulta SQL no banco de dados Northwind e extrai as informações de todos os itens dos pedidos. Em seguida, é realizada uma operação de merge entre as informações extraídas e o arquivo CSV gerado pela tarefa anterior. A operação de merge é realizada utilizando a função merge (inner Join) do Pandas. Ao final, é contada a quantidade de itens pedidos na cidade do Rio de Janeiro e o resultado é armazenado em um arquivo txt.

* export_final_output:

  Realiza a exportação do resultado final da quantidade de itens pedidos na cidade do Rio de Janeiro em texto codificado.

# Fluxo de trabalho

* A primeira tarefa de extração (task_id: 'northwind_extract') extrai informações do banco de dados Northwind, que está localizado no caminho 'airflow_data/data/Northwind_small.sqlite', através de uma consulta SQL. A seguir, ela gera um arquivo CSV (output_orders.csv) com as informações extraídas.

* A segunda tarefa de extração (task_id: 'northwind_extract_ord_det') executa outra consulta SQL no banco de dados Northwind e extrai as informações de todos os itens dos pedidos. Em seguida, lê o arquivo CSV gerado pela task anterior e executa uma operação de junção (inner join) utilizando Pandas. O resultado dessa operação é armazenado em um arquivo txt (count.txt), contendo a quantidade de itens pedidos que foram enviados para a cidade do Rio de Janeiro.

* A terceira tarefa de exportação (task_id: 'export_final_output') lê o arquivo count.txt gerado na tarefa anterior e exporta o resultado para um arquivo final em base64 (final_output.txt). Além disso, envia um e-mail contendo o resultado final e o endereço de e-mail do remetente é armazenado em uma variável do Airflow chamada 'my_email'.

# Instruções de uso
* Certifique-se de ter o Airflow instalado e configurado corretamente.

* Certifique-se de ter o banco de dados Northwind_small.sqlite em seu diretório.

* Baixe o arquivo etl_northwind.py e coloque-o no diretório de dags do Airflow.

* Certifique-se de que os caminhos de arquivo especificados nas tarefas de extração correspondam aos caminhos em seu sistema.

* Inicie o Airflow Standalone, execute a DAG e aguarde sua conclusão.





