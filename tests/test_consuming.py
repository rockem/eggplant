from threading import Thread
from time import sleep

from busypie import wait, FIVE_SECONDS
from kombu import Connection, Exchange, Producer

from eggplant.core import Eggplant
from eggplant.kumbo import RabbitKombuConsumer


def test_consume_using_function_handler():
    app = Eggplant(RabbitKombuConsumer(amqp_uri='amqp://localhost', exchange='eggplant-exchange', queue='test_queue'))
    received_messages = []

    @app.handler('status_changed')
    def status_changed_handler(message):
        received_messages.append(message)

    Thread(target=app.start).start()
    sleep(1)
    _send_message('status_changed', 'enabled')
    wait().at_most(FIVE_SECONDS).until(lambda: 'enabled' in received_messages)
    app.stop()


def _send_message(topic, message):
    with Connection('amqp://localhost') as conn:
        exchange = Exchange("eggplant-exchange", type="topic")
        Producer(exchange=exchange, channel=conn.channel(), routing_key=topic).publish(message)
