#!/usr/bin/env python
import pika

class EWalletPublisher():
    def __init__(self, queue_url, exchange_name):
        self.queue_url = queue_url
        self.exchange_name = exchange_name
        self.connection = None
        self.credentials = None

    def connect(self):
        print("queue_url={}".format(self.queue_url))
        self.credentials = pika.PlainCredentials('sisdis', 'sisdis')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.queue_url, credentials=self.credentials))
        self.channel = self.connection.channel()

    def publish(self, message):
        self.channel.queue_declare(queue='queue')
        self.channel.basic_publish(exchange=self.exchange_name,
                                   routing_key='',
                                   body=message)
        self.connection.close()
        print("Sent hello")

    def run_ping(self):
        pass


ewallet_publisher = EWalletPublisher('172.17.0.3', 'EX_PINGWIN')
ewallet_publisher.connect()
ewallet_publisher.publish('hello world! nyem_irfan')
