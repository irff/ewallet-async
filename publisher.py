#!/usr/bin/env python
import json
import pika
import schedule
import threading
import time
from datetime import datetime

SENDER_ID = '1306398983'

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

        print("Publishing PING message ({}) to exchange {}".format(message, self.ex_ping))

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

    def publish_register_request(self, user_id, nama, routing_key):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.queue_url,
                                                                       credentials=self.credentials))
        channel = connection.channel()

        channel.exchange_declare(exchange=self.ex_register,
                                 exchange_type='direct')

        message = self._build_register_request_message(user_id, nama)
        channel.basic_publish(exchange=self.ex_register,
                              routing_key=routing_key,
                              body=message)

        print("Publishing REGISTER message ({}), to exchange {}, routing key {}".format(message, self.ex_register, routing_key))

        connection.close()

