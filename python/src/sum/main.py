import os
import logging
import threading

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
        self.sum_control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, ["EOFs"]
        )

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.fruit_sum_by_client = {}

        self.lock = threading.Lock()

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

        for final_fruit_item in client_map.values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
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
            self._process_eof(client_id)
            ack()
        else:
            logging.warning(f"Unknown message format: {fields}")
            nack()
            return
            

    def process_data_messsage(self, message, ack, nack):

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
                self.sum_control_exchange.send(message_protocol.internal.serialize([fields[0]]))
            except Exception as e:
                logging.error("Failed to publish control EOF: %s", e)
            ack()
            return

        else:
            logging.warning(f"Unknown message format: {fields}")
            return


    def start(self):
        control_thread = threading.Thread(target = lambda: 
                            self.sum_control_exchange.start_consuming(self._control_callback), 
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
