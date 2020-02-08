from threading import Thread
from time import sleep

import pytest
from busypie import wait, FIVE_SECONDS
from kombu import Connection, Exchange, Producer, Queue, binding
from kombu.mixins import ConsumerMixin

from eggplant.core import Eggplant
from eggplant.kombu import RabbitKombuConsumer

RABBIT_URI = 'amqp://localhost'
EXCHANGE = 'eggplant-exchange'


@pytest.fixture(scope='module')
def app():
    eggplant = Eggplant(
        RabbitKombuConsumer(amqp_uri=RABBIT_URI, exchange='eggplant-exchange', queue='test_queue'))
    yield eggplant
    eggplant.stop()


def test_consume_using_function_handler(app):
    received_messages = []

    @app.handler('status_changed')
    def status_changed_handler(message):
        received_messages.append(message)

    Thread(target=app.start).start()
    sleep(1)
    _send_message('status_changed', 'enabled')
    wait().at_most(FIVE_SECONDS).until(lambda: 'enabled' in received_messages)


def test_produce_message_by_topic(app):
    with Connection(RABBIT_URI) as conn:
        message_consumer = MessagesConsumer(conn)
        Thread(target=message_consumer.run).start()

    app.publish(topic='user_logged_in', message='userID')
    wait().at_most(FIVE_SECONDS).until(lambda: 'userID' in message_consumer.received_messages)
    message_consumer.stop()


def _send_message(topic, message):
    with Connection(RABBIT_URI) as conn:
        exchange = Exchange(EXCHANGE, type="topic")
        Producer(exchange=exchange, channel=conn.channel(), routing_key=topic).publish(message)


class MessagesConsumer(ConsumerMixin):
    received_messages = []

    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, consumer, channel):
        exchange = Exchange(EXCHANGE, type="topic")
        queue = Queue(
            'my_queue',
            exchange,
            bindings=[binding(exchange, routing_key='user_logged_in')])
        return [
            consumer([queue], callbacks=[self.on_message]),
        ]

    def on_message(self, body, message):
        self.received_messages.append(body)
        message.ack()

    def stop(self):
        self.should_stop = True
