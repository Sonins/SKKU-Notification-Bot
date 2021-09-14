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
from typing import Dict

from airflow import DAG
from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago
from operators.discord_bot_operator import DiscordBotOperator


def build_slack_block_message(date: str, **context) -> str:
    postJson = context["ti"].xcom_pull(task_ids="parse_response")
    message = []
    date = date.split("-")
    message.append(
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{date[0]}년 {date[1]}월 {date[2]}일 {len(postJson)}개의 새 공지사항",
            },
        }
    )

    for post in postJson:
        p = {}
        p["type"] = "section"
        p["text"] = {"type": "mrkdwn", "text": f'[{post["title"]}]({post["link"]})'}
        message.append(p)
    return json.dumps(message)


def build_discord_message(date: str, **context) -> str:
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


def parse_notice(date: str, **context) -> Dict:
    try:
        conn = Connection.get_connection_from_secrets("skku_cs_http")
        hostname = conn.host
    except AirflowNotFoundException as err:
        raise err

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

slackBuildMessageOperator = PythonOperator(
    task_id="slack_build_message",
    python_callable=build_slack_block_message,
    op_kwargs={"date": "{{ ds }}"},
    provide_context=True,
    do_xcom_push=True,
    dag=dag,
)

slackSendNotificationOperator = SlackWebhookOperator(
    task_id="slack_send_message",
    http_conn_id="slack_noti_bot",
    channel="#airflow_feature_test",
    blocks="{{ ti.xcom_pull(task_ids='slack_build_message') }}",
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
    dag=dag,
)

SKKUScraper >> parsingOperator
parsingOperator >> slackBuildMessageOperator >> slackSendNotificationOperator
parsingOperator >> discordBuildMessageOperator >> discordSendNotificationOperator
