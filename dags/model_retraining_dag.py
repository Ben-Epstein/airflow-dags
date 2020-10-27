# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import print_function

import time
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

args = {
    'owner': 'Ben',
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id='RF_Churn_Retraining',
    default_args=args,
    schedule_interval=None,
    tags=['example', 'k8s']
)


# [START howto_operator_python]
def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


run_this = PythonOperator(
    task_id='Validate_Data',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)
hyper1 =  PythonOperator(
    task_id='Hyper Param Comb 1',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

hyper2 =  PythonOperator(
    task_id='Hyper Param Comb 2',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

hyper3 =  PythonOperator(
    task_id='Hyper Param Comb 3',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

hyper4 =  PythonOperator(
    task_id='Hyper Param Comb 4',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)
check_winner = PythonOperator(
    task_id='Check Winner',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

compare_to_production = PythonOperator(
    task_id='Compare to Prod',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

deploy_if_better = PythonOperator(
    task_id='Deploy if Better',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

run_this >> [hyper1, hyper2, hyper3, hyper4]
[hyper1, hyper2, hyper3, hyper4] >> check_winner
check_winner >> compare_to_production
compare_to_production >> deploy_if_better

