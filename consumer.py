#!/usr/bin/env python
import json
import pika
import schedule
import threading
import time
from datetime import datetime
from tinydb import TinyDB, Query


class EWalletConsumer():
    def __init__(self, queue_url, exchange_name, npm):
        self.queue_url = queue_url
        self.exchange_name = exchange_name
        self.credentials = pika.PlainCredentials('sisdis', 'sisdis')
        self.npm = npm
        self.db = TinyDB('ping_db.json')
        self.DB = Query()

    def _update_db(self, message):
        try:
            message = json.loads(message)
            result = self.db.search(self.DB.action == message['action'] and \
                                    self.DB.npm == message['npm'])

            if len(result) > 0:
                self.db.update({
                    'ts': message['ts']
                }, self.DB.action == message['action'] and \
                   self.DB.npm == message['npm'])
                print("DB updated: {}".format(message))
            else:
                self.db.insert(message)
                print("DB inserted: {}".format(message))
        except Exception as e:
            print("Error updating DB: ".format(e.message))

    def _ping_callback(self, ch, method, properties, body):
        print("Ping received: {}".format(body))
        self._update_db(body)

    def consume_ping(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.queue_url,
                                                                       credentials=self.credentials))
        channel = connection.channel()

        channel.exchange_declare(exchange=self.exchange_name,
                                 exchange_type='fanout')

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=self.exchange_name,
                           queue=queue_name)
        channel.basic_consume(self._ping_callback,
                              queue=queue_name,
                              no_ack=True)
        channel.start_consuming()


ewallet_consumer = EWalletConsumer('172.17.0.3', 'EX_PING', '1306398983')
ewallet_consumer.consume_ping()
