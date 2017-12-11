#!/usr/bin/env python
import json
import pika

from publisher import EWalletPublisher
from tinydb import TinyDB, Query

class EWalletConsumer():
    def __init__(self, queue_url, npm, publisher):
        self.queue_url = queue_url
        self.credentials = pika.PlainCredentials('sisdis', 'sisdis')
        self.npm = npm

        self.ex_ping =  'EX_PING'
        self.ex_register = 'EX_REGISTER'
        self.ex_saldo = 'EX_GET_SALDO'
        self.ex_transfer = 'EX_TRANSFER'
        self.ex_total_saldo = 'EX_GET_TOTAL_SALDO'
        self.publisher = publisher

        self.db = TinyDB('db.json')
        self.DB = Query()

    def _quorum_check(self):
        return True

    def _has_registered(self, user_id):
        result = self.db.search(self.DB.user_id == user_id and self.DB.nilai_saldo.exists())
        if len(result) > 0:
            return True
        return False

    def _retrieve_saldo(self, user_id):
        result = self.db.search(self.DB.user_id == user_id and self.DB.nilai_saldo.exists())
        if len(result) > 0:
            return int(result[0]['nilai_saldo'])
        return -1

    def _add_saldo(self, user_id, nilai):
        result = self.db.search(self.DB.user_id == user_id and self.DB.nilai_saldo.exists())
        if len(result) > 0:
            initial_value = result[0]['nilai_saldo']
            final_value = int(initial_value) + int(nilai)
            self.db.update({
                'nilai_saldo': final_value
            }, self.DB.user_id == user_id)
            return 1
        return -4

    # message = dict
    def _update_db(self, message):
        result = self.db.search(self.DB.user_id == message['user_id'])

        if len(result) > 0:
            self.db.update({
                'ts': message['ts']
            }, self.DB.user_id == message['user_id'])
            # print("DB updated: {}".format(message))
        else:
            self.db.insert(message)
            # print("DB inserted: {}".format(message))

    def _ping_callback(self, ch, method, properties, body):
        # print("PING received: {}".format(body))
        body = json.loads(body)

        message = {
            'user_id': body['npm'],
            'ts': body['ts']
        }

        self._update_db(message)

    def _register_response_callback(self, ch, method, properties, body):
        print('Received REGISTER RESPONSE: {}'.format(body))

    def _register_request_callback(self, ch, method, properties, body):
        print('Received REGISTER REQUEST: {}'.format(body))

        body = json.loads(body)
        sender_id = body['sender_id']

        try:
            message = {
                'user_id': body['user_id'],
                'nama': body['nama'],
                'nilai_saldo': 0
            }

            if self._quorum_check():
                if not self._has_registered(body['user_id']):
                    self._update_db(message)
                    status_register = 1
                else:
                    status_register = -4
            else:
                status_register = -2
        except:
            status_register = -99

        self.publisher.publish_register_response(status_register=status_register, sender_id=sender_id)

    def _saldo_response_callback(self, ch, method, properties, body):
        print('Received GET SALDO RESPONSE: {}'.format(body))


    def _saldo_request_callback(self, ch, method, properties, body):
        print('Received GET SALDO REQUEST: {}'.format(body))

        body = json.loads(body)
        sender_id = body['sender_id']

        try:
            if self._quorum_check():
                nilai_saldo = self._retrieve_saldo(body['user_id'])
            else:
                nilai_saldo = -2
        except:
            nilai_saldo = -99

        self.publisher.publish_saldo_response(nilai_saldo=nilai_saldo, sender_id=sender_id)

    def _transfer_response_callback(self, ch, method, properties, body):
        print('Received TRANSFER RESPONSE: {}'.format(body))

    def _transfer_request_callback(self, ch, method, properties, body):
        print('Received TRANSFER REQUEST: {}'.format(body))

        body = json.loads(body)
        sender_id = body['sender_id']

        try:
            if self._quorum_check():
                status_transfer = self._add_saldo()
            else:
                status_transfer = -2
        except:
            status_transfer = -99


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

    def _consume_direct(self, routing_key, exchange_name, callback):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.queue_url,
                                                                       credentials=self.credentials))
        channel = connection.channel()

        channel.exchange_declare(exchange=exchange_name,
                                 exchange_type='direct',
                                 durable=True)

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=exchange_name,
                           queue=queue_name,
                           routing_key=routing_key)
        channel.basic_consume(consumer_callback=callback,
                              queue=queue_name,
                              no_ack=True)
        channel.start_consuming()

    def consume_register_response(self):
        routing_key = 'RESP_{}'.format(self.npm)
        self._consume_direct(routing_key, self.ex_register, self._register_response_callback)

    def consume_register_request(self):
        routing_key = 'REQ_{}'.format(self.npm)
        self._consume_direct(routing_key, self.ex_register, self._register_request_callback)

    def consume_saldo_response(self):
        routing_key = 'RESP_{}'.format(self.npm)
        self._consume_direct(routing_key, self.ex_saldo, self._saldo_response_callback)

    def consume_saldo_request(self):
        routing_key = 'REQ_{}'.format(self.npm)
        self._consume_direct(routing_key, self.ex_saldo, self._saldo_request_callback)
