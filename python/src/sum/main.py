import os
import logging
import threading
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        try:
            self.sum_control_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, 
                SUM_CONTROL_EXCHANGE, 
                ["EOFs"]
            )
            self.sum_control_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, 
                SUM_CONTROL_EXCHANGE, 
                ["EOFs"]
            )
        except Exception as e:
            logging.exception("Failed to create Sum Exchange Control Middleware")
            raise

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.fruit_sum_by_client = {}

        self.lock = threading.Lock()
        self.inflight = 0
        self.pending_eofs = set()

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        with self.lock:
            client_map = self.fruit_sum_by_client.setdefault(client_id, {})
            client_map[fruit] = client_map.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages")
        with self.lock:
            client_map = self.fruit_sum_by_client.get(client_id, {})

        # Select Aggregator to send client data
        h = int(hashlib.md5(client_id.encode()).hexdigest(), 16)
        idx = h % AGGREGATION_AMOUNT
        data_output_exchange = self.data_output_exchanges[idx]

        for final_fruit_item in client_map.values():
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        logging.info(f"Broadcasting EOF message")
        data_output_exchange.send(message_protocol.internal.serialize([client_id]))

        with self.lock:
            self.fruit_sum_by_client.pop(client_id, None)


    def _control_callback(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
        except Exception as e:
            logging.error("Failed when deserializing message: %s", e)
            nack()
            return

        if len(fields) == 1:
            client_id = fields[0]
            logging.info(f"Control EOF received for client {client_id}")
            with self.lock:
                if self.inflight > 0:
                    logging.info("Currently processing data. Deferring EOF.")
                    self.pending_eofs.add(client_id)
                    ack()
                    return
            self._process_eof(client_id)
            ack()
        else:
            logging.warning(f"Unknown message format: {fields}")
            nack()
            return
            

    def process_data_messsage(self, message, ack, nack):

        with self.lock:
            self.inflight += 1

        try:
            try:
                fields = message_protocol.internal.deserialize(message)
            except Exception as e:
                logging.error("Failed when deserializing message: %s", e)
                nack()
                return

            if len(fields) == 3:
                self._process_data(*fields)
                ack()

            elif len(fields) == 1:
                client_id = fields[0]
                logging.info(f"Publishing control EOF for client {client_id}")
                try:
                    self.sum_control_publisher.send(message_protocol.internal.serialize([fields[0]]))
                    ack() 
                    return
                except Exception as e:
                    logging.exception("Failed to publish control EOF")
                    nack()
                    return

            else:
                logging.warning(f"Unknown message format: {fields}")
                nack()
                return
        finally:
            with self.lock:
                self.inflight -= 1
                if self.inflight == 0:
                    pending = list(self.pending_eofs)
                    self.pending_eofs.clear()
                else:
                    pending = []

        for client_id in pending:
            self._process_eof(client_id)                    

    def start(self):
        control_thread = threading.Thread(target = lambda: 
                            self.sum_control_consumer.start_consuming(self._control_callback), 
                            daemon=True
        ) # Mas tarde hacer un graceful shutdown handleando sigterm
        control_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)
        

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
