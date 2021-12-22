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
from ast import literal_eval
from typing import Dict, Iterable, Optional, Union

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from operators.discord_bot_operator import DiscordBotOperator


def _build_author_object(name: str, icon_url: str = ""):
    obj = {}
    obj["name"] = name
    if icon_url:
        obj["icon_url"] = icon_url
    return obj


def _build_message_helper(date: str, posts: Iterable[Dict]) -> str:
    """
    Build notification message for Discord.
    :type date: str
    :type posts: Iterable[Dict]
    """
    message = {}
    message["content"] = f"**{date} : {len(posts)}개의 공지사항이 있습니다.**"
    message["embeds"] = []

    for post in posts:
        elem = {}
        elem["type"] = "link"
        elem["title"] = post["title"]
        elem["author"] = _build_author_object("공지사항 봇")
        elem["url"] = post["link"]

        message["embeds"].append(elem)

    return json.dumps(message)


def build_discord_noti(date: str, post: Union[str, Iterable[Dict]], **context) -> str:
    """
    디스코드의 메시지 양식에 맞추어 Json 형태의 메시지를 만들어 주는 함수입니다.
    Embed 오브젝트를 사용합니다. 각 포스트 하나당 Embed 오브젝트 하나를 사용합니다.
    (https://discord.com/developers/docs/resources/channel#create-message)

    :type post: Union[str, Iterable[Dict]]
    :param post: The post you want to notificate.
                 It should be json string or iterable object of Dict.
                 It should contains property 'title' and 'link'.
    """
    if isinstance(post, str):
        d = literal_eval(post)
        if isinstance(d, Iterable):
            post = d
        elif isinstance(d, Dict):
            post = [d]

    return _build_message_helper(date, post)


def discord_post_notify(
    http_conn_id: str,
    post: Union[str, Iterable[Dict]],
    dag: DAG,
    channel: str = "",
    date: Optional[str] = None,
    **kwargs,
) -> TaskGroup:
    """
    Notify new post to Discord using DiscordBotOperator.
    Build message using PythonOpreator first and send it.
    """
    discord_post_notify_task_group = TaskGroup(group_id="discord_post_notify", dag=dag)

    discordBuildMessageOperator = PythonOperator(
        task_id="discord_build_message",
        task_group=discord_post_notify_task_group,
        python_callable=build_discord_noti,
        op_kwargs={"date": date, "post": post},
        provide_context=True,
        do_xcom_push=True,
        dag=dag,
    )

    discordSendNotificationOperator = DiscordBotOperator(
        task_id="discord_send_message",
        task_group=discord_post_notify_task_group,
        http_conn_id=http_conn_id,
        json="{{ ti.xcom_pull(task_ids='discord_post_notify.discord_build_message') }}",
        channel=channel,
        dag=dag,
    )

    discordBuildMessageOperator >> discordSendNotificationOperator
    return discord_post_notify_task_group
