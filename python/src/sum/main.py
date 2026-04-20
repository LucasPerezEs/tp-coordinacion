import os
import logging
import threading
import hashlib
import signal

from sender import Sender
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

        self.sender = Sender(MOM_HOST, AGGREGATION_PREFIX, AGGREGATION_AMOUNT)

        self.shutdown_event = threading.Event()

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
        with self.lock:
            client_map = self.fruit_sum_by_client.pop(client_id, {})

        # Select Aggregator to send client data
        h = int(hashlib.md5(client_id.encode()).hexdigest(), 16)
        idx = h % AGGREGATION_AMOUNT


        # Build payload
        payloads = []
        for final_fruit_item in client_map.values():
            payloads.append(
                message_protocol.internal.serialize([
                    client_id, final_fruit_item.fruit, final_fruit_item.amount
                ])
            )
        
        # Append EOF
        payloads.append(message_protocol.internal.serialize([client_id]))

        try:
            logging.info("Sending batch for client %s to aggregator %d (items=%d)", client_id, idx, len(client_map))
            self.sender.send(idx, payloads, timeout=10)
            logging.info("Batch sent for client %s to aggregator %d", client_id, idx)
        except Exception:
            logging.exception("Failed to send batch for client %s, restoring state", client_id)
            # restore the client data so we can retry later
            with self.lock:
                existing = self.fruit_sum_by_client.setdefault(client_id, {})
                for fruit, item in client_map.items():
                    existing[fruit] = existing.get(fruit, fruit_item.FruitItem(fruit, 0)) + item
            raise


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
            try:
                self._process_eof(client_id)
            except Exception:
                nack()
                return
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
                except Exception as e:
                    logging.exception("Failed to publish control EOF")
                    nack()
                    return
                ack() 

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

    def _on_sigterm(self, signum, frame):
        logging.info("SIGTERM received, initiating graceful shutdown")
        # mark shutdown so other logic can check it if necessary
        try:
            self.shutdown_event.set()
        except Exception:
            pass

        # stop consumers so start_consuming can return
        try:
            self.input_queue.stop_consuming()
        except Exception:
            pass
        try:
            self.sum_control_consumer.stop_consuming()
        except Exception:
            pass

        # stop sender to avoid further publishes
        try:
            self.sender.stop()
        except Exception:
            pass                 

    def start(self):
        self.sender.start()
        control_thread = threading.Thread(target = lambda: 
                            self.sum_control_consumer.start_consuming(self._control_callback), 
                            daemon=True
        ) # Mas tarde hacer un graceful shutdown handleando sigterm
        control_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)
        

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, lambda s, f: sum_filter._on_sigterm(s, f))
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
