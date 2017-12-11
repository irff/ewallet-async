import threading
from publisher import EWalletPublisher
from consumer import EWalletConsumer


class EWalletRunner():
    def __init__(self):
        self.publisher = EWalletPublisher('172.17.0.3', '1306398983')
        self.consumer = EWalletConsumer('172.17.0.3', '1306398983')

        self.publisher.publish_ping()
        self.consumer.consume_ping()

        consume_ping_thread = threading.Thread(target=self.consumer.consume_ping)
        consume_ping_thread.start()

    def do_register(self, user_id, nama, receiver):
        req_routing_key = 'REQ_{}'.format(receiver)
        resp_routing_key = 'RESP_{}'.format(receiver)

        self.publisher.publish_register_request(user_id, nama, req_routing_key)
        self.consumer.consume_register_response(resp_routing_key)

runner = EWalletRunner()
