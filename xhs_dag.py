from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests

MYSQL_CONN_ID = 'mysql'
MYSQL_DATABASE = 'test1'
MYSQL_TABLE = 'xiaohongshu_posts'

def fetch_posts(url, **kwargs):
    r = requests.get(url)

    html_content = r.text

    soup = BeautifulSoup(html_content, 'html.parser')

    keywords = soup.find('meta', attrs={'name': 'keywords'})
    description = soup.find('meta', attrs={'name': 'description'})
    span_tag = soup.find('span', class_='username')

    tag = keywords['content'] if keywords else 'No keywords found'
    post = description['content'] if description else 'No description found'
    text_length = len(post)
    if span_tag:
        username = span_tag.get_text()
    else:
        username = 'No <span> tag with class "username" found'
    sql = f"INSERT INTO xiaohongshu_posts (username, post, tag, text_length) VALUES ('{username}', '{post}', '{tag}', '{text_length}');"

    kwargs['ti'].xcom_push(key='sql_insert', value=sql)

MYSQL_CONN_ID = 'mysql'
MYSQL_DATABASE = 'test1'
MYSQL_TABLE = 'xiaohongshu_posts'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'xiaohongshu_etl',
    default_args=default_args,
    description='A simple ETL DAG for Xiaohongshu posts',
    schedule=timedelta(days=1),
)

fetch_posts_task = PythonOperator(
    task_id='fetch_posts',
    python_callable=fetch_posts,
    op_kwargs={'url': 'https://www.xiaohongshu.com/explore/66a378d100000000270105fc?xsec_token=AB86BIBgd2dQHlQwOcP3i-SU3NFZbcov_PCIDbBky5vOQ=&xsec_source=pc_feed'},
    # provide_context=True,
    dag=dag,
)

store_posts_task = MySqlOperator(
    task_id='store_posts_task',
    mysql_conn_id='mysql_conn_id', 
    sql="{{ ti.xcom_pull(task_ids='fetch_posts', key='sql_insert') }}",
    dag=dag,
)

fetch_posts_task >> store_posts_task