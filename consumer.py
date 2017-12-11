#!/usr/bin/env python
import json
import pika

from tinydb import TinyDB, Query


class EWalletConsumer():
    def __init__(self, queue_url, npm):
        self.queue_url = queue_url
        self.credentials = pika.PlainCredentials('sisdis', 'sisdis')
        self.npm = npm

        self.ex_ping =  'EX_PING'
        self.ex_register = 'EX_REGISTER'
        self.ex_saldo = 'EX_GET_SALDO'
        self.ex_transfer = 'EX_TRANSFER'
        self.ex_total_saldo = 'EX_GET_TOTAL_SALDO'

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
        print("PING received: {}".format(body))
        self._update_db(body)

    def _register_response_callback(self, ch, method, properties, body):
        print("REGISTER response received: {}".format(body))

    def consume_ping(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.queue_url,
                                                                       credentials=self.credentials))
        channel = connection.channel()

        channel.exchange_declare(exchange=self.ex_ping,
                                 exchange_type='fanout')

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=self.ex_ping,
                           queue=queue_name)
        channel.basic_consume(self._ping_callback,
                              queue=queue_name,
                              no_ack=True)
        channel.start_consuming()

    def consume_register_response(self, routing_key):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.queue_url,
                                                                       credentials=self.credentials))
        channel = connection.channel()

        channel.exchange_declare(exchange=self.ex_register,
                                 exchange_type='direct',
                                 durable=True)

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=self.ex_register,
                           queue=queue_name,
                           routing_key=routing_key)
        channel.basic_consume(self._register_response_callback,
                              queue=queue_name,
                              no_ack=True)
        channel.start_consuming()

