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

import logging
from typing import Dict, List

from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago

from airflow import DAG
from notibot.task_group.discord_notify import discord_post_notify


def parse_notice(date: str = "", **context) -> List[Dict]:
    """
    크롤링 된 공지사항 포스트 중 date 날짜에 작성된 공지사항만 필터링합니다.
    이때, 해당 공지사항 포스트의 주소를 link 속성에 추가해줍니다.

    :type date: str
    :param date: The date when post you want is written.
    """
    import json

    try:
        conn = Connection.get_connection_from_secrets("skku_cs_http")
        hostname = conn.host
    except AirflowNotFoundException as err:
        logging.error(f"{str(err)}")

    response = context["ti"].xcom_pull(task_ids="skku_cs_scrap")
    responseDict = json.loads(response)

    for res in responseDict["aaData"]:
        res["link"] = f'{hostname}/ko/news/notice/view/{res["id"]}'

    if date:
        noticeList = list(
            filter(
                lambda elem: elem["time"] == date.replace("-", "."),
                responseDict["aaData"],
            )
        )
    else:
        noticeList = list(responseDict["aaData"])

    return noticeList


def post_exists(**context) -> str:
    """
    Checks if there are any posts exist.
    return "exists" or "does_not_exists" corresponding to result.
    """
    posts = context["ti"].xcom_pull(task_ids="parse_response")
    if len(posts) != 0:
        return "exists"
    else:
        return "does_not_exists"


args = {
    "owner": "Sonins",
    "email": ["gmlrhks95@gmail.com"],
    "email_on_failure": True,
}

dag = DAG(
    dag_id="skku_notice_bot_dag",
    schedule_interval="0 0 * * *",
    start_date=days_ago(0),
    max_active_runs=1,
    tags=["notice", "bot", "skku"],
    default_args=args,
)

SKKUScraper = SimpleHttpOperator(
    task_id="skku_cs_scrap",
    endpoint="/rest/board/list/notice",
    method="GET",
    http_conn_id="skku_cs_http",
    do_xcom_push=True,
    dag=dag,
)

parsingOperator = PythonOperator(
    task_id="parse_response",
    python_callable=parse_notice,
    op_kwargs={"date": "{{ ds }}"},
    provide_context=True,
    do_xcom_push=True,
    dag=dag,
)

checkPostExsitsOperator = BranchPythonOperator(
    task_id="check_post_exists",
    python_callable=post_exists,
    provide_context=True,
)

postExistsOperator = DummyOperator(task_id="exists")

postNotExistsOperator = DummyOperator(task_id="does_not_exists")

discordNotifyTaskGroup = discord_post_notify(
    http_conn_id="discord_noti_bot",
    date="{{ ds }}",
    post="{{ ti.xcom_pull(task_ids='parse_response') }}",
    channel="885386005274828801",
    dag=dag,
)

SKKUScraper >> parsingOperator >> checkPostExsitsOperator
checkPostExsitsOperator >> postNotExistsOperator
checkPostExsitsOperator >> postExistsOperator
postExistsOperator >> discordNotifyTaskGroup
