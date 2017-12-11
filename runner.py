import threading
from publisher import EWalletPublisher
from consumer import EWalletConsumer


class EWalletRunner():
    def __init__(self):
        self.publisher = EWalletPublisher('172.17.0.3', '1306398983')
        self.consumer = EWalletConsumer('172.17.0.3', '1306398983', self.publisher)

        publish_ping_thread = threading.Thread(target=self.publisher.publish_ping)
        publish_ping_thread.start()

        consume_ping_thread = threading.Thread(target=self.consumer.consume_ping)
        consume_ping_thread.start()

        consumer_register_request_thread = threading.Thread(
            target=self.consumer.consume_register_request
        )
        consumer_register_request_thread.start()

        consume_register_response_thread = threading.Thread(
            target=self.consumer.consume_register_response
        )
        consume_register_response_thread.start()

    def do_register(self, user_id, nama, receiver_id):

        self.publisher.publish_register_request(user_id, nama, receiver_id)
