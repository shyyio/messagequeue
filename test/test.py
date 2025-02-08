from unittest import TestCase

from messagequeue import RedisMQ, parse_routing_key, RoutingKeyParts, get_redis


class TestRedisMQ(TestCase):

    def setUp(self) -> None:
        self.rdb = get_redis()
        self.rdb.delete("pending.test", "test_mq", "arc.test.msg.x", "pending.redis_mq")

    def test_ack(self):
        mq = RedisMQ(self.rdb, consumer_name="test", max_pending_time=2, arc_lists=["arc"])

        mq.publish({"_id": 1}, item_project="test", item_type="msg")

        topic1, msg1, ack1 = next(mq.read_messages(topics=["arc.*"]))

        self.assertEqual(self.rdb.hlen("pending.test"), 1)

        ack1()

        self.assertEqual(self.rdb.hlen("pending.test"), 0)

    def test_pending_timeout(self):
        mq = RedisMQ(self.rdb, consumer_name="test", max_pending_time=0.5, arc_lists=["arc"], wait=0)

        mq.publish({"_id": 1}, item_project="test", item_type="msg")

        topic1, msg1, ack1 = next(mq.read_messages(topics=["arc.test.*"]))

        self.assertEqual(self.rdb.hlen("pending.test"), 1)

        # msg1 will timeout after 0.5s, next iteration takes ceil(0.5)s
        topic1_, msg1_, ack1_ = next(mq.read_messages(topics=["arc.test.*"]))
        self.assertEqual(self.rdb.hlen("pending.test"), 1)

        ack1_()

        self.assertEqual(self.rdb.hlen("pending.test"), 0)

        self.assertEqual(msg1, msg1_)

    def test_no_id_field(self):
        mq = RedisMQ(self.rdb, consumer_name="test", max_pending_time=0.5, arc_lists=["arc"], wait=0)

        with self.assertRaises(ValueError):
            mq.publish({"a": 1}, item_project="test", item_type="msg")


class TestRoutingKey(TestCase):

    def test1(self):
        key = "arc.chan.4chan.post.b"
        parts = parse_routing_key(key)
        self.assertEqual(parts, RoutingKeyParts("arc", "chan", "4chan", "post", "b"))

    def test2(self):
        key = "arc.reddit.submission.birdpics"
        parts = parse_routing_key(key)
        self.assertEqual(parts, RoutingKeyParts("arc", "reddit", None, "submission", "birdpics"))
