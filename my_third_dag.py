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
    dag_id='email_test',
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
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)
# [END howto_operator_python]
def send_email(ds, **kwargs):
    from os import environ as env_vars
    import smtplib, ssl
    smtp_server = "smtp.gmail.com"
    port = 465
    sender_email = "bensairflow@gmail.com"
    receiver_email = env_vars['receiver']
    password = env_vars['pwd']
    message = 'Hey buddy. I sent this from Airflow just for you'
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)

send_email = PythonOperator(
    task_id='send_the_email',
    provide_context=True,
    python_callable=send_email,
    dag=dag,
)
send_email << run_this
