from kombu import Connection, Exchange, Queue, binding, Producer
from kombu.mixins import ConsumerMixin


class Worker(ConsumerMixin):
    def __init__(self, connection, queues, callback):
        self.connection = connection
        self._queues = queues
        self._callback = callback

    def get_consumers(self, consumer, channel):
        return [consumer(queues=self._queues,
                         callbacks=[self.on_message])]

    def on_message(self, body, message):
        self._callback(
            body,
            {'topic': message.delivery_info['routing_key']})
        message.ack()


class RabbitKombuConsumer:

    def __init__(self, amqp_uri, exchange, queue):
        self._amqp_uri = amqp_uri
        self._exchange = exchange
        self._queue = queue
        self._worker = None

    def consume(self, topics, callback):
        queue = self._create_queue_for(topics)
        with Connection(self._amqp_uri, heartbeat=4) as conn:
            self._worker = Worker(conn, [queue], callback)
            self._worker.run()

    def _create_queue_for(self, topics):
        exchange = Exchange(self._exchange, type="topic")
        queue = Queue(
            self._queue,
            exchange,
            bindings=[binding(exchange, routing_key=t) for t in topics])
        return queue

    def stop(self):
        self._worker.should_stop = True

    def publish(self, topic, message):
        with Connection(self._amqp_uri) as conn:
            exchange = Exchange(self._exchange, type="topic")
            Producer(exchange=exchange, channel=conn.channel(), routing_key=topic).publish(message)
