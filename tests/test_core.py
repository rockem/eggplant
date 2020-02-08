from eggplant.core import Eggplant


def test_consume_with_class_handler():
    broker = MockBroker()
    app = Eggplant(broker)
    received_messages = []

    @app.handler('password_expired')
    class StubHandler:
        def handle(self, message):
            received_messages.append(message)

    app.start()
    broker.dispatch_message('password_expired', 'password')
    assert 'password' in received_messages


class MockBroker:
    _callback = None

    def dispatch_message(self, topic, message):
        self._callback(message, {'topic': topic})

    def consume(self, topics, callback):
        self._callback = callback

    def stop(self):
        pass

    def publish(self, topic, message):
        pass
