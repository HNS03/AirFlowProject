from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow import settings
from airflow.models import Connection
from airflow.operators.email_operator import EmailOperator


default_args = {
    'email_on_failure': False,
    'pool': 'my_pool'            
}

dag = DAG(
      dag_id='customer360_pipeline',
      start_date=days_ago(1),
      default_args=default_args     
)
# sensor to check availability of file in s3 location
sensor = HttpSensor(
    task_id = 'watch_for_orders',
    http_conn_id = 'order_s3',
    endpoint = 'orders.csv',
    response_check = lambda response: response.status_code == 200,
    retry_delay =timedelta(minutes=5),
    retries =12,
    dag = dag
)
def get_order_url():
    session = settings.Session()
    connection = session.query(Connection).filter(Connection.conn_id == 'order_s3').first()
    return f'{connection.schema}://{connection.host}/orders.csv'
# creates directory and downloads order.csv
download_order_cmd = f'rm -rf airflow_pipeline && mkdir -p airflow_pipeline && cd airflow_pipeline && wget {get_order_url()}'
# SSH connection to itversity
download_to_edgenode = SSHOperator(
    task_id = 'download_orders',
    ssh_conn_id = 'itversity',
    command = download_order_cmd,
    dag = dag
)
# sqoop command to fetch customer data from mySQL
def fetch_customer_info_cmd():
    command_one = "hive -e 'DROP TABLE airflow_hani.customers'"
    command_one_ext = 'hdfs dfs -rm -R -f customers'
    command_two = "sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
    --username retail_user --password itversity --table customers \
    --hive-import --hive-database airflow_hani --hive-table customers"
    command_three = "exit 0"
    return f'{command_one} && {command_one_ext} && ({command_two} || {command_three})'
# SSH command to connect to itversity and run sqoop command
import_customer_info = SSHOperator(
    task_id = 'download_customers',
    ssh_conn_id = 'itversity',
    command = fetch_customer_info_cmd(),
    dag = dag
)
# HDFS command to put orders.csv from local to hdfs in itversity
upload_order_info = SSHOperator(
    task_id = 'upload_orders_to_hdfs',
    ssh_conn_id = 'itversity',
    command = 'hdfs dfs -rm -R -f airflow_input && hdfs dfs -mkdir -p airflow_input && hdfs dfs -put ./airflow_pipeline/orders.csv airflow_input',
    dag = dag
)
# spark program execution
def get_order_filter_cmd():
    command_zero = 'export SPARK_MAJOR_VERSION=2'
    command_one = 'hdfs dfs -rm -R -f /user/itv002708/airflow_output'
    command_two = 'spark-submit --class AirflowPractice /home/itv002708/AirflowPractice.jar /user/itv002708/airflow_input/orders.csv /user/itv002708/airflow_output'
    return f'{command_zero} && {command_one} && {command_two}'
# spark program initiation
process_order_info = SSHOperator(
    task_id = 'process_orders',
    ssh_conn_id = 'itversity',
    command = get_order_filter_cmd(),
    dag = dag
)
def create_order_hive_table_cmd():
    command_one = '''hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS airflow_hani.orders(order_id int, order_date string, order_customer_id int, order_status string) row format delimited fields terminated by ',' stored as  textfile location '/user/itv002708/airflow_ouput'"'''
    return f'{command_one}'
# hive table creation
create_order_table = SSHOperator(
    task_id = 'create_orders_table_hive',
    ssh_conn_id = 'itversity',
    command = create_order_hive_table_cmd(),
    dag = dag
)
def load_hbase_cmd():
    command_one = '''hive -e "CREATE TABLE IF NOT EXISTS airflow_hani.airflow_hbase(customer_id int, customer_fname string, customer_lname string, order_id int, order_date string) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' with SERDEPROPERTIES('hbase.columns.mapping'=':key,personal:customer_fname, personal:customer_lname, personal:order_id, personal: order_date')"'''
    command_two = 'hive -e "INSERT OVERWRITE TABLE airflow_hani.airflow_hbase select c.customer_id, c.customer_fname, c.customer_lname, o.order_id, o.order_date from airflow_hani.customers c join airflow_hani.orders o ON (c.customer_id=o.order_customer_id)"'
    return f'{command_one} && {command_two}'
# HBase table creation
load_hbase = SSHOperator(
    task_id = 'load_hbase_table',
    ssh_conn_id = 'itversity',
    command = load_hbase_cmd(),
    dag = dag
)

success_notify = EmailOperator(
    task_id='sucess_email_notify',
    to='airflowpractice03@gmail.com',
    subject='Pipeline ingestion COMPLETED',
    html_content=""" <h1>Congratulations! Hbase data is ready.</h1> """,
    trigger_rule='all_success',
    dag=dag
)

failure_notify = EmailOperator(
    task_id='failure_email_notify',
    to='airflowpractice03@gmail.com',
    subject='Pipeline ingestion has FAILED',
    html_content=""" <h1>Sorry! Hbase data is not yet ready.</h1> """,
    trigger_rule='all_failed',
    dag=dag
)

dummy = DummyOperator(
    task_id = 'dummy',
    dag = dag
)
sensor >> import_customer_info

sensor >> download_to_edgenode >> upload_order_info >> process_order_info >> create_order_table

[import_customer_info, create_order_table] >> load_hbase >> [success_notify, failure_notify] >>dummy