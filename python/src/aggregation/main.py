import os
import logging
import signal
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.amounts_by_client = {}  # {client_id: {fruit: FruitItem}}
        self.eof_count_by_client = {}  # {client_id: 0}

        self.shutdown = threading.Event()

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")

        client_map = self.amounts_by_client.setdefault(client_id, {})
        client_map[fruit] = client_map.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
        
        # Increment EOF counter for this client
        self.eof_count_by_client.setdefault(client_id, 0)
        self.eof_count_by_client[client_id] += 1

        # Wait until all Sum replicas send their EOFs
        if self.eof_count_by_client[client_id] < SUM_AMOUNT:
            logging.info("Not all EOFs for this client arrived yet: %u/%u", self.eof_count_by_client[client_id], SUM_AMOUNT)
            return

        # Build top fruits from accumulated map for this client
        client_map = self.amounts_by_client.get(client_id, {})
        items = list(client_map.values())
        items.sort(reverse=True)
        top_items = items[:TOP_SIZE]
        fruit_top = [(it.fruit, it.amount) for it in top_items]

        logging.info("Partial built for %s: %s", client_id, fruit_top)

        # Send result to Join instance with client_id and agg_id
        try:
            self.output_queue.send(message_protocol.internal.serialize([client_id, ID, fruit_top]))
            logging.info(f"Sending partial top of client {client_id}")
        except Exception as e:
            logging.exception("Error while sending partial top of client %s", client_id)
            raise

        # Cleanup
        self.amounts_by_client.pop(client_id, None)
        self.eof_count_by_client.pop(client_id, None)

    def process_message(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
            ack()
        elif len(fields) == 1:
            try:
                self._process_eof(fields[0])
                ack()
            except Exception:
                nack()
                return
        else:
            logging.warning(f"Unknown message format: {fields}")
            nack()
            return

    def _on_sigterm(self, signum, frame):
        logging.info("SIGTERM received, shutting down aggregation")
        self.shutdown.set()
        try:
            self.input_exchange.stop_consuming()
        except Exception:
            pass

    def _cleanup(self):
        try: 
            self.output_queue.close()
        except: 
            pass
        try: 
            self.input_exchange.close()
        except: 
            pass

    def start(self):
        signal.signal(signal.SIGTERM, lambda s,f: self._on_sigterm(s,f))
        try:
            self.input_exchange.start_consuming(self.process_message)
        finally:
            self._cleanup()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
