#!/usr/bin/env python
import json
import pika
import schedule
import threading
import time
from datetime import datetime


class EWalletPublisher():
    def __init__(self, queue_url, exchange_name, npm):
        self.queue_url = queue_url
        self.exchange_name = exchange_name
        self.credentials = pika.PlainCredentials('sisdis', 'sisdis')
        self.npm = npm

    def _get_ping_message(self):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        message = {
            'action': 'ping',
            'npm': self.npm,
            'ts': timestamp
        }

        return json.dumps(message)

    def _ping(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.queue_url,
                                                                       credentials=self.credentials))
        channel = connection.channel()

        channel.exchange_declare(exchange=self.exchange_name,
                                 exchange_type='fanout')

        message = self._get_ping_message()
        channel.basic_publish(exchange=self.exchange_name,
                              routing_key='',
                              body=message)

        print("Sending message ({}) to exchange {}".format(message, self.exchange_name))

        connection.close()

    def _run_threaded(self, job_func):
        job_thread = threading.Thread(target=job_func)
        job_thread.start()

    def publish_ping(self):
        try:
            schedule.every(5).seconds.do(self._run_threaded, self._ping())
            while 1:
                schedule.run_pending()
                time.sleep(5)
        except Exception as e:
            print("Error running ping {}".format(e.message))


ewallet_publisher = EWalletPublisher('172.17.0.3', 'EX_PING', '1306398983')
ewallet_publisher.publish_ping()
