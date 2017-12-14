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

        self.transfer_response_connection = None
        self.transfer_user_id = None
        self.transfer_nilai = None

    def _get_neighbors(self):
        return [
            '1306398983',  # irfan
            '1406579100',  # wahyu
            '1406543826',
            '1406527513',
            '1406543845',  # gilang
            # '1406543574',  # oda
            '1406559055',  # ghozi
            # '1406572025',  # adit
            '1406543883',  # jefly
            '1406559036',  # gales
        ]

    def _get_active_neighbors(self):
        print('Checking QUORUM')
        neighbors = self._get_neighbors()
        active = []

        for neighbor in neighbors:
            try:
                result = self.db.get((self.DB.user_id == neighbor) & (self.DB.ts.exists()))

                if result is not None:
                    ts_now = datetime.now()
                    ts_neighbor_str = result['ts']
                    ts_neighbor = datetime.strptime(ts_neighbor_str, '%Y-%m-%d %H:%M:%S')

                    ts_diff = (ts_now - ts_neighbor).seconds
                    print('PING Time diff {}: {} seconds'.format(neighbor, ts_diff))
                    if ts_diff <= 10:
                        active.append(neighbor)
                else:
                    print('PING Not found {}'.format(neighbor))
            except Exception as e:
                print('Error retrieving from db: {}'.format(e.message))

        return active

    def _quorum_check(self):
        quorum = len(self._get_active_neighbors())
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

    def _update_saldo(self, user_id, nilai):
        result = self.db.get((self.DB.user_id == user_id) & (self.DB.nilai_saldo.exists()))
        if result is not None:
            initial_value = result['nilai_saldo']
            final_value = int(initial_value) + int(nilai)
            self.db.update({
                'nilai_saldo': final_value
            }, self.DB.user_id == user_id)

            print('Updating {}\'s saldo from {} to {}', user_id, initial_value, final_value)
            return 1
        print('Failed updating saldo. User id {} not found.'.format(user_id))
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

    def _timeout_callback(self, ch, method, properties, body):
        print('Not receiving any messages after 2 seconds timeout. Disconnecting channel.')
        if self.transfer_response_connection:
            self.transfer_response_connection.close()
            self.transfer_response_connection = None

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

        body = json.loads(body)
        action = body['action']
        status_transfer = int(body['status_transfer'])

        if action == 'transfer':
            if status_transfer == 1:
                if self.transfer_user_id and self.transfer_nilai:
                    # subtract current saldo
                    self._update_saldo(user_id=self.transfer_user_id,
                                       nilai=-self.transfer_nilai)

                    ch.connection.close()
                    self.transfer_user_id = None
                    self.transfer_nilai = None

    def _transfer_request_callback(self, ch, method, properties, body):
        print('Received TRANSFER REQUEST: {}'.format(body))

        body = json.loads(body)
        sender_id = body['sender_id']

        try:
            if self._quorum_check() >= HALF_QUORUM:
                status_transfer = self._update_saldo(body['user_id'], body['nilai'])
            else:
                status_transfer = -2
        except:
            status_transfer = -99

        self.publisher.publish_transfer_response(status_transfer=status_transfer, sender_id=sender_id)

    def _total_saldo_response_callback(self, ch, method, properties, body):
        print('Received GET TOTAL SALDO RESPONSE: {}'.format(body))

    def _total_saldo_request_callback(self, ch, method, properties, body):
        print('Received GET TOTAL SALDO REQUEST: {}'.format(body))

        body = json.loads(body)
        sender_id = body['sender_id']
        user_id = body['user_id']

        active_neighbors = self._get_active_neighbors()
        neighbor_count = len(active_neighbors)

        if neighbor_count >= HALF_QUORUM:

            consumer = TotalSaldoConsumer(queue_url=self.queue_url,
                                          npm=self.npm,
                                          publisher=self.publisher,
                                          neighbor_count=neighbor_count)

            consumer.consume_saldo_response_total()

            for neighbor in active_neighbors:
                print('Sending GET SALDO REQUEST to: {}'.format(neighbor))
                self.publisher.publish_saldo_request(user_id, neighbor)

        else:
            nilai_saldo = -2
            self.publisher.publish_total_saldo_response(nilai_saldo=nilai_saldo,
                                                        sender_id=sender_id)

        nilai_saldo = -99
        self.publisher.publish_total_saldo_response(nilai_saldo=nilai_saldo,
                                                        sender_id=sender_id)

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

    def _consume_response(self, routing_key, exchange_name, callback):
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

        self.transfer_response_connection = connection
        connection.add_timeout(2, self._timeout_callback)
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
        self._consume_response(routing_key, self.ex_transfer, self._transfer_response_callback)

    def consume_total_saldo_request(self):
        routing_key = 'REQ_{}'.format(self.npm)
        self._consume_direct(routing_key, self.ex_total_saldo, self._total_saldo_request_callback)

    def consume_total_saldo_response(self):
        routing_key = 'RESP_{}'.format(self.npm)
        self._consume_direct(routing_key, self.ex_total_saldo, self._total_saldo_response_callback)


class TotalSaldoConsumer():
    def __init__(self, queue_url, npm, publisher, neighbor_count):
        self.queue_url = queue_url
        self.credentials = pika.PlainCredentials('sisdis', 'sisdis')
        self.npm = npm
        self.ex_saldo = 'EX_GET_SALDO'
        self.publisher = publisher

        self.neighbor_count = neighbor_count

        self.total_saldo = 0

    def _saldo_total_response_callback(self, ch, method, properties, body):
        print('Received GET SALDO RESPONSE (TOTAL): {}'.format(body))

        body = json.loads(body)
        nilai_saldo = int(body['nilai_saldo'])

        print('USER_COUNT={}'.format(self.neighbor_count))
        self.neighbor_count -= 1

        if nilai_saldo not in [-1, -2, -4, -99]:
            self.total_saldo += nilai_saldo

        if self.neighbor_count <= 0:
            print('Closing connection')
            ch.connection.close()
            self.publisher.publish_total_saldo_response(nilai_saldo=self.total_saldo,
                                                        sender_id=self.npm)

    def consume_saldo_response_total(self):
        print('Consuming saldo response total')
        routing_key = 'RESP_{}'.format(self.npm)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.queue_url,
                                                                       credentials=self.credentials))
        channel = connection.channel()

        channel.exchange_declare(exchange=self.ex_saldo,
                                 exchange_type='direct',
                                 durable=True)

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=self.ex_saldo,
                           queue=queue_name,
                           routing_key=routing_key)
        channel.basic_consume(self._saldo_total_response_callback,
                              queue=queue_name,
                              no_ack=True)
        print('Finish consuming saldo response total')
        channel.start_consuming()
        print('Finish consuming saldo response total')
