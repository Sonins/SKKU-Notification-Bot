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

import json
import logging
from typing import Dict, List

from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from operators.discord_bot_operator import DiscordBotOperator

from airflow import DAG


def build_discord_message(date: str, **context) -> str:
    """
    디스코드의 메시지 양식에 맞추어 Json 형태의 메시지를 만들어 주는 함수입니다.
    Embed 오브젝트를 사용합니다. 각 포스트 하나당 Embed 오브젝트 하나를 사용합니다.
    (https://discord.com/developers/docs/resources/channel#create-message)

    :type date: str
    :param date: The date when post you want is written.
    """
    postJson = context["ti"].xcom_pull(task_ids="parse_response")

    message = {}
    message["content"] = f"**{date} : {len(postJson)}개의 공지사항이 있습니다.**"
    message["embeds"] = []

    for post in postJson:
        elem = {}
        elem["type"] = "link"
        elem["title"] = post["title"]
        elem["url"] = post["link"]

        message["embeds"].append(elem)

    return json.dumps(message)


def parse_notice(date: str, **context) -> List[Dict]:
    """
    크롤링 된 공지사항 포스트 중 date 날짜에 작성된 공지사항만 필터링합니다.
    이때, 해당 공지사항 포스트의 주소를 link 속성에 추가해줍니다.

    :type date: str
    :param date: The date when post you want is written.
    """
    try:
        conn = Connection.get_connection_from_secrets("skku_cs_http")
        hostname = conn.host
    except AirflowNotFoundException as err:
        logging.error(f"{str(err)}")

    response = context["ti"].xcom_pull(task_ids="skku_cs_scrap")
    responseDict = json.loads(response)

    for res in responseDict["aaData"]:
        res["link"] = f'{hostname}/ko/news/notice/view/{res["id"]}'

    noticeList = list(
        filter(
            lambda elem: elem["time"] == date.replace("-", "."), responseDict["aaData"]
        )
    )

    return noticeList


args = {
    "owner": "Sonins",
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

discordBuildMessageOperator = PythonOperator(
    task_id="discord_build_message",
    python_callable=build_discord_message,
    op_kwargs={"date": "{{ ds }}"},
    provide_context=True,
    do_xcom_push=True,
    dag=dag,
)

discordSendNotificationOperator = DiscordBotOperator(
    task_id="discord_send_message",
    http_conn_id="discord_noti_bot",
    json="{{ ti.xcom_pull(task_ids='discord_build_message') }}",
    channel="887674028368752650",
    dag=dag,
)

SKKUScraper >> parsingOperator
parsingOperator >> discordBuildMessageOperator >> discordSendNotificationOperator
