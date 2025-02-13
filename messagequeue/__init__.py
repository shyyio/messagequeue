import json
from collections import namedtuple
from functools import partial
from itertools import islice
from time import sleep, time

from orjson import orjson
from redis import Redis
import os


def get_redis():
    return Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", 6379))
    )


RoutingKeyParts = namedtuple(
    "RoutingKeyParts",
    ["arc_list", "project", "subproject", "type", "category"]
)


def parse_routing_key(key):
    tokens = key.split(".")

    if len(tokens) == 4:
        arc_list, project, type_, category = tokens
        return RoutingKeyParts(
            arc_list=arc_list,
            project=project,
            subproject=None,
            type=type_,
            category=category
        )
    else:
        arc_list, project, subproject, type_, category = tokens
        return RoutingKeyParts(
            arc_list=arc_list,
            project=project,
            subproject=subproject,
            type=type_,
            category=category
        )



class RedisMQ:
    _MAX_KEYS = 30

    def __init__(self, rdb, topic, sep=".", max_pending_time=120, logger=None, wait=1):
        self._rdb: Redis = rdb
        self._topic = topic
        self._key_cache = None
        self._pending_list = f"topic{sep}{topic}"
        self._max_pending_time = max_pending_time
        self._logger = logger
        self._wait = wait

    def _get_keys(self):
        if self._key_cache:
            return self._key_cache

        keys = list(islice(
            self._rdb.scan_iter(match=self._topic, count=RedisMQ._MAX_KEYS), RedisMQ._MAX_KEYS
        ))
        self._key_cache = keys

        return keys

    def _get_pending_tasks(self):
        for task_id, pending_task in self._rdb.hscan_iter(self._pending_list):

            pending_task_json = orjson.loads(pending_task)

            if time() >= pending_task_json["resubmit_at"]:
                yield pending_task_json["topic"], pending_task_json["task"], partial(self._ack, task_id)

    def _ack(self, task_id):
        self._rdb.hdel(self._pending_list, task_id)

    def read_messages(self):
        """
        Assumes json-encoded tasks with an _id field

        Tasks are automatically put into a pending list until ack() is called.
        When a task has been in the pending list for at least max_pending_time seconds, it
        gets submitted again
        """

        counter = 0

        if self._logger:
            self._logger.info(f"MQ>Listening for new messages in {self._topic}")

        while True:
            counter += 1

            if counter % 1000 == 0:
                yield from self._get_pending_tasks()

            keys = self._get_keys()
            if not keys:
                sleep(self._wait)
                self._key_cache = None
                continue

            result = self._rdb.blpop(keys, timeout=1)
            if not result:
                self._key_cache = None
                continue

            topic, task = result

            task_json = orjson.loads(task)
            topic = topic.decode()

            if "_id" not in task_json or not task_json["_id"]:
                raise ValueError(f"Task doesn't have _id field: {task}")

            # Immediately put in pending queue
            self._rdb.hset(
                self._pending_list, task_json["_id"],
                orjson.dumps({
                    "resubmit_at": time() + self._max_pending_time,
                    "topic": topic,
                    "task": task_json
                })
            )

            yield topic, task_json, partial(self._ack, task_json["_id"])

    def publish(self, item):

        item = json.dumps(item, separators=(',', ':'), ensure_ascii=False, sort_keys=True)

        self._rdb.lpush(self._topic, item)
