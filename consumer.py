#!/usr/bin/env python
import json
import pika
from datetime import datetime

from publisher import EWalletPublisher
from tinydb import TinyDB, Query

FULL_QUORUM = 8
HALF_QUORUM = 5
NO_QUORUM = 0

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

    def _get_neighbors(self):
        return [
            '1306398983',  # irfan
            '1406579100',  # wahyu
            '1406543574',  # oda
            '1406543845', # gilang
            '1406559055', # ghozi
            '1406572025',  # adit
            '1406543883',  # jefly
            '1406559036', # gales
        ]

    def _quorum_check(self):
        neighbors = self._get_neighbors()
        quorum = 0
        for neighbor in neighbors:
            result = self.db.get((self.DB.user_id == neighbor))

            if result is not None:
                ts_now = datetime.now()
                ts_neighbor_str = result['ts']
                ts_neighbor = datetime.strptime(ts_neighbor_str, '%Y-%m-%d %H:%M:%S')

                ts_diff = (ts_now - ts_neighbor).seconds
                print('PING Time diff {}: {} seconds'.format(neighbor, ts_diff))
                if ts_diff <= 10:
                    quorum += 1
            else:
                print('PING Not found {}'.format(neighbor))

        print('QUORUM={}'.format(quorum))

        return quorum

    def _has_registered(self, user_id):
        result = self.db.get((self.DB.user_id == user_id) & (self.DB.nilai_saldo.exists()))
        if result is not None:
            print("{} has registered".format(user_id))
            return True
        return False

    def _retrieve_saldo(self, user_id):
        result = self.db.get((self.DB.user_id == user_id) & (self.DB.nilai_saldo.exists()))
        if result is not None:
            value = int(result['nilai_saldo'])
            print("Retrieving saldo of {}, value {}".format(user_id, value))
            return value
        return -1

    def _add_saldo(self, user_id, nilai):
        result = self.db.get((self.DB.user_id == user_id) & (self.DB.nilai_saldo.exists()))
        if result is not None:
            initial_value = result['nilai_saldo']
            final_value = int(initial_value) + int(nilai)
            self.db.update({
                'nilai_saldo': final_value
            }, self.DB.user_id == user_id)
            return 1
        return -4

    # message = dict
    def _update_db(self, message):
        result = self.db.get(self.DB.user_id == message['user_id'])

        if result is not None:
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

            if self._quorum_check() >= HALF_QUORUM:
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
            if self._quorum_check() >= HALF_QUORUM:
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
            if self._quorum_check() >= HALF_QUORUM:
                status_transfer = self._add_saldo(body['user_id'], body['nilai'])
            else:
                status_transfer = -2
        except:
            status_transfer = -99

        self.publisher.publish_transfer_response(status_transfer=status_transfer, sender_id=sender_id)

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

    def consume_transfer_request(self):
        routing_key = 'REQ_{}'.format(self.npm)
        self._consume_direct(routing_key, self.ex_transfer, self._transfer_request_callback)

    def consume_transfer_response(self):
        routing_key = 'RESP_{}'.format(self.npm)
        self._consume_direct(routing_key, self.ex_transfer, self._transfer_response_callback)

