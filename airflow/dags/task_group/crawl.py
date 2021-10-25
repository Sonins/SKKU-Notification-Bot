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
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup


def parse_notice(hostname: str, response: str, date: str = "", **context) -> List[Dict]:
    """
    크롤링 된 공지사항 포스트 중 date 날짜에 작성된 공지사항만 필터링합니다.
    이때, 해당 공지사항 포스트의 주소를 link 속성에 추가해줍니다.

    :type date: str
    :param date: The date when post you want is written.
    """
    responseDict = json.loads(response)

    # Append url to actual webpage.
    for res in responseDict["aaData"]:
        res["link"] = f'{hostname}/ko/news/notice/view/{res["id"]}'

    # Filter posts by date
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


def crawl_skku_cs_notice(
    http_conn_id: str,
    dag: DAG,
    date: str = "",
) -> TaskGroup:

    try:
        conn = Connection.get_connection_from_secrets(http_conn_id)
        hostname = conn.host
    except AirflowNotFoundException as err:
        logging.error(f"{str(err)}")

    skku_cs_crawl = TaskGroup(group_id="skku_cs_crawl", dag=dag)

    SKKUCrawler = SimpleHttpOperator(
        task_id="crawl",
        endpoint="/rest/board/list/notice",
        method="GET",
        http_conn_id=http_conn_id,
        do_xcom_push=True,
        task_group=skku_cs_crawl,
        dag=dag,
    )

    parsingOperator = PythonOperator(
        task_id="parse_response",
        python_callable=parse_notice,
        op_kwargs={
            "hostname": hostname,
            "response": "{{ ti.xcom_pull(task_ids='skku_cs_crawl.crawl') }}",
            "date": date,
        },
        provide_context=True,
        do_xcom_push=True,
        task_group=skku_cs_crawl,
        dag=dag,
    )

    SKKUCrawler >> parsingOperator
    return skku_cs_crawl
