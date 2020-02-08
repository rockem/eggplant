import inspect


class Eggplant:
    def __init__(self, broker):
        self._broker = broker
        self._handlers = {}

    def handler(self, topic):
        def decorator_handler(func):
            self._handlers.update({topic: func})

        return decorator_handler

    def start(self):
        self._broker.consume(topics=self._handlers.keys(), callback=self._on_message)

    def _on_message(self, message, delivery_info):
        handler = self._handlers[delivery_info['topic']]
        if inspect.isclass(handler):
            handler().handle(message)
        else:
            handler(message)

    def stop(self):
        self._broker.stop()

    def publish(self, topic, message):
        self._broker.publish(topic, message)
