#!/usr/bin/env python
import json
import pika
import schedule
import threading
import time
from datetime import datetime

class EWalletPublisher():
    def __init__(self, queue_url, npm):
        self.queue_url = queue_url
        self.credentials = pika.PlainCredentials('sisdis', 'sisdis')
        self.npm = npm
        self.ex_ping =  'EX_PING'
        self.ex_register = 'EX_REGISTER'
        self.ex_saldo = 'EX_GET_SALDO'
        self.ex_transfer = 'EX_TRANSFER'
        self.ex_total_saldo = 'EX_GET_TOTAL_SALDO'

    def _get_timestamp(self):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return timestamp

    def _build_ping_message(self):
        message = {
            'action': 'ping',
            'npm': self.npm,
            'ts': self._get_timestamp()
        }

        return json.dumps(message)

    def _build_register_request_message(self, user_id, nama):
        message = {
            'action': 'register',
            'user_id': user_id,
            'nama': nama,
            'sender_id': self.npm,
            'type': 'request',
            'ts': self._get_timestamp()
        }

        return json.dumps(message)

    def _build_register_response_message(self, status_register):
        message = {
            'action': 'register',
            'type': 'response',
            'status_register': status_register,
            'ts': self._get_timestamp()
        }

        return json.dumps(message)

    def _build_saldo_response_message(self, nilai_saldo):
        message = {
            'action': 'get_saldo',
            'type': 'response',
            'nilai_saldo': nilai_saldo,
            'ts': self._get_timestamp()
        }

        return json.dumps(message)

    def _ping(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.queue_url,
                                                                       credentials=self.credentials))
        channel = connection.channel()

        channel.exchange_declare(exchange=self.ex_ping,
                                 exchange_type='fanout')

        message = self._build_ping_message()
        channel.basic_publish(exchange=self.ex_ping,
                              routing_key='',
                              body=message)

        # print("Publishing PING message ({}) to exchange {}".format(message, self.ex_ping))

        connection.close()

    def _run_threaded(self, job_func):
        job_thread = threading.Thread(target=job_func)
        job_thread.start()

    def publish_ping(self):
        try:
            schedule.every(5).seconds.do(self._run_threaded, self._ping)

            while True:
                schedule.run_pending()
                time.sleep(1)

        except Exception as e:
            print("Error running ping {}".format(e.message))

    def publish_direct(self, exchange, routing_key, message):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.queue_url,
                                                                       credentials=self.credentials))
        channel = connection.channel()

        channel.exchange_declare(exchange=exchange,
                                 exchange_type='direct',
                                 durable=True)

        channel.basic_publish(exchange=exchange,
                              routing_key=routing_key,
                              body=message)

        connection.close()

    def publish_register_request(self, user_id, nama, receiver_id):
        routing_key = 'REQ_{}'.format(receiver_id)
        message = self._build_register_request_message(user_id, nama)

        self.publish_direct(self.ex_register, routing_key, message)

        print("Published REGISTER REQUEST message ({}), to exchange {}, routing key {}".format(message,
                                                                                               self.ex_register,
                                                                                               routing_key))


    def publish_register_response(self, status_register, sender_id):
        routing_key = 'RESP_{}'.format(sender_id)
        message = self._build_register_response_message(status_register)

        self.publish_direct(self.ex_register, routing_key, message)

        print("Published REGISTER RESPONSE message ({}), to exchange {}, routing key {}".format(message, self.ex_register, routing_key))

    def publish_saldo_response(self, nilai_saldo, sender_id):
        routing_key = 'RESP_{}'.format(sender_id)
        message = self._build_saldo_response_message(nilai_saldo)

        self.publish_direct(self.ex_saldo, routing_key, message)
        print("Published GET SALDO RESPONSE message ({}), to exchange {}, routing key {}".format(message,
                                                                                                self.ex_saldo,
                                                                                                routing_key))
