"""Queue semantics: fan-out, leases, ack, nack, visibility timeout, DLQ, replay."""

from __future__ import annotations


def test_publish_then_receive_returns_payload_and_headers(queue):
    queue.subscribe("t", "g")
    queue.publish("t", {"a": 1}, key="k", headers={"correlation_id": "c1"})
    [d] = queue.receive("t", "g")
    assert d.message.payload == {"a": 1}
    assert d.message.headers["correlation_id"] == "c1"
    assert d.message.key == "k"
    assert d.attempts == 1


def test_topic_without_subscribers_creates_no_deliveries(queue):
    queue.publish("nobody.listens", {"a": 1})
    assert queue.depth() == []


def test_every_group_gets_every_message(queue):
    queue.subscribe("t", "g1")
    queue.subscribe("t", "g2")
    queue.publish("t", {"n": 1})
    assert len(queue.receive("t", "g1")) == 1
    assert len(queue.receive("t", "g2")) == 1


def test_competing_consumers_in_one_group_do_not_share_a_lease(queue):
    queue.subscribe("t", "g")
    queue.publish("t", {"n": 1})
    first = queue.receive("t", "g")
    second = queue.receive("t", "g")
    assert len(first) == 1 and second == []


def test_fifo_order_within_a_topic(queue):
    queue.subscribe("t", "g")
    for i in range(5):
        queue.publish("t", {"n": i})
    got = [d.message.payload["n"] for d in queue.receive("t", "g", max_messages=5)]
    assert got == [0, 1, 2, 3, 4]


def test_ack_removes_message_for_good(queue, clock):
    queue.subscribe("t", "g")
    queue.publish("t", {})
    [d] = queue.receive("t", "g", visibility_timeout=1)
    assert queue.ack(d) is True
    clock.advance(100)
    assert queue.receive("t", "g") == []


def test_unacked_message_is_redelivered_after_visibility_timeout(queue, clock):
    queue.subscribe("t", "g")
    queue.publish("t", {"x": 1})
    [d1] = queue.receive("t", "g", visibility_timeout=10)
    clock.advance(9)
    assert queue.receive("t", "g") == []
    clock.advance(2)
    [d2] = queue.receive("t", "g", visibility_timeout=10)
    assert d2.message.id == d1.message.id
    assert d2.attempts == 2


def test_stale_receipt_cannot_ack_after_redelivery(queue, clock):
    queue.subscribe("t", "g")
    queue.publish("t", {})
    [d1] = queue.receive("t", "g", visibility_timeout=1)
    clock.advance(2)
    [d2] = queue.receive("t", "g", visibility_timeout=1)
    assert queue.ack(d1) is False
    assert queue.ack(d2) is True


def test_nack_with_delay_hides_message_until_delay_passes(queue, clock):
    queue.subscribe("t", "g")
    queue.publish("t", {})
    [d] = queue.receive("t", "g")
    queue.nack(d, delay=5)
    assert queue.receive("t", "g") == []
    clock.advance(5)
    assert len(queue.receive("t", "g")) == 1


def test_nack_on_last_attempt_moves_to_dlq(queue):
    queue.subscribe("t", "g")
    queue.publish("t", {"bad": True})
    for _ in range(3):  # max_attempts=3 in the fixture
        [d] = queue.receive("t", "g")
        queue.nack(d, error="boom")
    assert queue.receive("t", "g") == []
    [dl] = queue.dead_letters()
    assert dl.attempts == 3 and dl.last_error == "boom"


def test_crashing_consumer_is_dead_lettered_after_max_attempts(queue, clock):
    queue.subscribe("t", "g")
    queue.publish("t", {})
    for _ in range(3):
        assert len(queue.receive("t", "g", visibility_timeout=1)) == 1
        clock.advance(2)  # never ack: the worker "died"
    assert queue.receive("t", "g") == []
    [dl] = queue.dead_letters()
    assert "visibility timeout" in dl.last_error


def test_nack_dead_letter_skips_retries(queue):
    queue.subscribe("t", "g")
    queue.publish("t", {})
    [d] = queue.receive("t", "g")
    queue.nack(d, error="poison", dead_letter=True)
    assert len(queue.dead_letters()) == 1


def test_replay_returns_dead_letters_with_fresh_attempts(queue):
    queue.subscribe("t", "g")
    queue.publish("t", {"n": 1})
    [d] = queue.receive("t", "g")
    queue.nack(d, dead_letter=True)
    assert queue.replay(topic="t") == 1
    [d2] = queue.receive("t", "g")
    assert d2.attempts == 1 and queue.dead_letters() == []


def test_depth_counts_by_state(queue):
    queue.subscribe("t", "g")
    for _ in range(3):
        queue.publish("t", {})
    [d] = queue.receive("t", "g")
    queue.ack(d)
    [d] = queue.receive("t", "g")
    states = {r["state"]: r["count"] for r in queue.depth()}
    assert states == {"acked": 1, "inflight": 1, "ready": 1}


def test_messages_survive_reopening_the_database(db_path, clock):
    from orderpipeline.queue.sqlite import SQLiteQueue

    q1 = SQLiteQueue(db_path, clock=clock)
    q1.subscribe("t", "g")
    q1.publish("t", {"durable": True})
    q1.close()
    q2 = SQLiteQueue(db_path, clock=clock)
    [d] = q2.receive("t", "g")
    assert d.message.payload == {"durable": True}
    q2.close()
