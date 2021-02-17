from airflow.models import Variable
#from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from splicemachinesa.pyodbc import splice_connect
from airflow.utils.dates import days_ago
from pprint import pprint
from airflow.models import DAG

def executeSQL(ds, **kwargs):
    pprint(ds)
    pprint(kwargs)
    cnx = splice_connect(UID=Variable.get('UID'),PWD=Variable.get('db_password'),HOST=Variable.get('host'),SSL="basic")
    cursor = cnx.cursor()
    cursor.execute('drop table if exists splice.foo')
    cursor.commit()
    cursor.execute('create table splice.foo(col1 int, col2 varchar(5000)')
    cursor.execute('insert into splice.foo values(55, \'test\'')
    cursor.commit()


dag = DAG(
    dag_id='test_SQL',
    default_args=None,
    schedule_interval=None,
    tags=['example SQL']
)

sqlOperator = PythonVirtualenvOperator(
  task_id='run the sql'
  provide_context=True,
  python_callable=executeSQL,
  dag=dag,
  requirements=['pyodbc','splicemachinesa']
  python_version='3.8'
)
