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

        # === REGISTER
        consume_register_request_thread = threading.Thread(
            target=self.consumer.consume_register_request
        )
        consume_register_request_thread.start()

        consume_register_response_thread = threading.Thread(
            target=self.consumer.consume_register_response
        )
        consume_register_response_thread.start()

        # === GET SALDO
        consume_saldo_request_thread = threading.Thread(
            target=self.consumer.consume_saldo_request
        )
        consume_saldo_request_thread.start()

        consume_saldo_response_thread = threading.Thread(
            target=self.consumer.consume_saldo_response
        )
        consume_saldo_response_thread.start()

        # === TRANSFER

        consume_transfer_request_thread = threading.Thread(
            target=self.consumer.consume_transfer_request
        )
        consume_transfer_request_thread.start()

        consume_transfer_response_thread = threading.Thread(
            target=self.consumer.consume_transfer_response
        )
        consume_transfer_response_thread.start()

        # === GET TOTAL SALDO
        consume_total_saldo_request_thread = threading.Thread(
            target=self.consumer.consume_total_saldo_request
        )
        consume_total_saldo_request_thread.start()

        consume_total_saldo_response_thread = threading.Thread(
            target=self.consumer.consume_total_saldo_response
        )
        consume_total_saldo_response_thread.start()

    def do_register(self, user_id, nama, receiver_id):
        self.publisher.publish_register_request(user_id, nama, receiver_id)

    def do_get_saldo(self, user_id, receiver_id):
        self.publisher.publish_saldo_request(user_id, receiver_id)

    def do_transfer(self, user_id, nilai, receiver_id):
        self.publisher.publish_transfer_request(user_id, nilai, receiver_id)

    def do_get_total_saldo(self, user_id, receiver_id):
        self.publisher.publish_total_saldo_request(user_id, receiver_id)
