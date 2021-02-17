from airflow.models import Variable
#from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
import requests
from airflow.utils.dates import days_ago
from pprint import pprint
from airflow.models import DAG

"""
def executeSQL(ds, **kwargs):
    from splicemachinesa.pyodbc import splice_connect
    pprint(ds)
    pprint(kwargs)
    cnx = splice_connect(UID=Variable.get('UID'),PWD=Variable.get('db_password'),HOST=Variable.get('host'),SSL="basic")
    cursor = cnx.cursor()
    cursor.execute('drop table if exists splice.foo')
    cursor.commit()
    cursor.execute('create table splice.foo(col1 int, col2 varchar(5000)')
    cursor.execute('insert into splice.foo values(55, \'test\'')
    cursor.commit()
"""

def executeSQL(ds, **kwargs):
  url = Variable.get('url')
  headers = {"username":Variable.get('UID'), "password":Variable.get('db_password')}
  data = {"sqlstmt":'drop table if exists splice.foo','autocommit':True}
  requests.post(url=url, headers=headers, data=data)
  data['sqlstmt'] = 'create table splice.foo(col1 int, col2 varchar(5000))'
  requests.post(url=url, headers=headers, data=data)
  data['sqlstmt'] = "insert into splice.foo values(55,'test')"
  requests.post(url=url, headers=headers, data=data)   

dag = DAG(
    dag_id='test_SQL',
    default_args=None,
    schedule_interval=None,
    tags=['example SQL']
)

sqlOperator = PythonOperator(
  task_id='run_the_sql',
  provide_context=True,
  python_callable=executeSQL,
  dag=dag,
  #requirements=['pyodbc','splicemachinesa'],
  #python_version='3.8',
  start_date=days_ago(2)
)
