# -*- coding: utf-8 -*-
from dtable_events.handlers import MessageHandler, EventHandler


class App(object):
    def __init__(self, config):
        self._message_handler = MessageHandler(config)
        self._event_handler = EventHandler(config)

    def serve_forever(self):
        self._message_handler.start()
        self._event_handler.start()
