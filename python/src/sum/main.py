import os
import logging
import threading
import hashlib
import signal
import time

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
SAFE_EOF_BACKOFF_SEC = 1
SAFE_EOF_MAX_RETRIES = 3
DATA_FIELDS = 3
CONTROL_FIELDS = 1

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
        self.inflight_by_client = {}
        self.pending_eofs = set()
        self.sent_eofs = set()
        self.flushing = set()

    def _process_data(self, client_id, fruit, amount):
        logging.info("Process data")
        with self.lock:
            if client_id in self.sent_eofs or client_id in self.flushing:
                logging.error("Data received for already flushed client %s", client_id)
                return

            client_map = self.fruit_sum_by_client.setdefault(client_id, {})
            client_map[fruit] = client_map.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))


    def _safe_wait_for_client_eof(self, client_id, attempts=SAFE_EOF_MAX_RETRIES, interval=SAFE_EOF_BACKOFF_SEC):
        """Return True if no inflight messages for client_id after rechecks, False if inflight appeared."""
        for _ in range(attempts):
            with self.lock:
                if self.inflight_by_client.get(client_id, 0) > 0:
                    return False
            time.sleep(interval)
        return True


    def _start_flush(self, client_id):
        """Atomically pop client data and mark client as flushing.
        Returns the popped client_map (may be empty dict).
        """
        with self.lock:
            client_map = self.fruit_sum_by_client.pop(client_id, {})
            self.flushing.add(client_id)
        return client_map
    

    def _finish_flush(self, client_id):
        """Mark client as fully flushed (move flushing -> sent_eofs) and clean up flushing set."""
        with self.lock:
            self.sent_eofs.add(client_id)
            if client_id in self.flushing:
                self.flushing.remove(client_id)


    def _restore_after_failed_flush(self, client_id, client_map):
        """Restore client_map into fruit_sum_by_client and clear flushing flag after a failed send."""
        with self.lock:
            if client_id in self.flushing:
                self.flushing.remove(client_id)
            existing = self.fruit_sum_by_client.setdefault(client_id, {})
            for fruit, item in client_map.items():
                existing[fruit] = existing.get(fruit, fruit_item.FruitItem(fruit, 0)) + item


    def _process_eof(self, client_id):
        """
        Flush a client's accumulated fruit totals to aggregators.

        Atomically take and mark the client's state as flushing, route each (client,fruit) 
        to an aggregator using hash(client_id:fruit), 
        append a control EOF to every aggregator, and publish per-aggregator batches via the Sender. 
        
        If any publish fails, restore the client's state and raise.
        """
        # Take ownership of this client's accumulated data and mark flushing
        client_map = self._start_flush(client_id)

        # Group payloads per aggregator idx based on hash(client_id:fruit)
        per_idx_payloads = {i: [] for i in range(AGGREGATION_AMOUNT)}
        for final_fruit_item in client_map.values():
            logging.info("Fruit item for client %s: %s", client_id, final_fruit_item)
            key = f"{client_id}:{final_fruit_item.fruit}"
            h = int(hashlib.md5(key.encode()).hexdigest(), 16)
            idx = h % AGGREGATION_AMOUNT

            per_idx_payloads[idx].append(
                message_protocol.internal.serialize([
                    client_id, final_fruit_item.fruit, final_fruit_item.amount
                ])
            )
        
        # Append EOF payload to every aggregator
        eof_payload = message_protocol.internal.serialize([client_id])
        for i in range(AGGREGATION_AMOUNT):
            per_idx_payloads[i].append(eof_payload)


        try:
            for idx in range(AGGREGATION_AMOUNT):
                payloads = per_idx_payloads[idx]
                logging.info("Sending batch for client %s to aggregator %d (items=%d)", client_id, idx, max(0, len(payloads) - 1))
                self.sender.send(idx, payloads, timeout=10)
                logging.info("Batch sent for client %s to aggregator %d", client_id, idx)
        except Exception:
            logging.exception("Failed to send batch for client %s, restoring state", client_id)
            self._restore_after_failed_flush(client_id, client_map)
            raise

        # mark as sent only after successful send
        self._finish_flush(client_id)


    def _control_callback(self, message, ack, nack):
        """
        Handle EOF control messages. 
        Defer if client has in-flight data, else wait briefly then flush.
        """

        # Deserialize message
        try:
            fields = message_protocol.internal.deserialize(message)
        except Exception as e:
            logging.error("Failed when deserializing message: %s", e)
            nack()
            return

        if len(fields) == CONTROL_FIELDS:
            client_id = fields[0]
            logging.info(f"Control EOF received for client {client_id}")
            with self.lock:
                if self.inflight_by_client.get(client_id, 0) > 0:
                    logging.info("Currently processing data. Deferring EOF.")
                    self.pending_eofs.add(client_id)
                    ack()
                    return

            if not self._safe_wait_for_client_eof(client_id):
                logging.info("Currently processing data. Deferring EOF.")
                self.pending_eofs.add(client_id)
                ack()
                return
                
            try:
                self._process_eof(client_id)
                ack()
            except Exception:
                logging.exception("Failed processing EOF of client %s", client_id)
                nack()

        else:
            logging.warning(f"Unknown message format: {fields}")
            nack()
            return
            

    def _process_data_message(self, message, ack, nack):
        """
        Process data or local EOF messages. 
        Maintain per-client in-flight counts and run deferred EOFs.
        """
        client_id = None
        eofs_to_process = []

        try:
            # Deserialize message
            try:
                fields = message_protocol.internal.deserialize(message)
            except Exception as e:
                logging.error("Failed when deserializing message: %s", e)
                nack()
                return

            if len(fields) == 0:
                return

            client_id = fields[0]

            # Increment inflight count for this client
            with self.lock:
                self.inflight_by_client[client_id] = self.inflight_by_client.get(client_id, 0) + 1

            if len(fields) == DATA_FIELDS:
                self._process_data(*fields)
                ack()

            elif len(fields) == CONTROL_FIELDS:
                logging.info(f"Publishing control EOF for client {client_id}")
                try:
                    self.sum_control_publisher.send(message_protocol.internal.serialize([client_id]))
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
        # decrement per-client inflight counter and collect any deferred EOFs to process
            with self.lock:
                if client_id is not None:
                    cnt = self.inflight_by_client.get(client_id, 0) - 1
                    if cnt <= 0:
                        # remove counter and trigger pending EOF for this client only if present
                        self.inflight_by_client.pop(client_id, None)
                        if client_id in self.pending_eofs:
                            self.pending_eofs.remove(client_id)
                            eofs_to_process.append(client_id)
                    else:
                        self.inflight_by_client[client_id] = cnt

            # process deferred EOFs outside the lock to avoid deadlock
            for cid in eofs_to_process:
                if self.shutdown_event.is_set():
                    logging.info("Shutdown in progress, deferring EOF for client %s", cid)
                    with self.lock:
                        self.pending_eofs.add(cid)
                    continue
                else:
                    try:
                        logging.info("Processing deferred EOF for client %s", cid)
                        self._process_eof(cid)
                    except Exception:
                        logging.exception("Deferred _process_eof failed for client %s", cid)


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
            self.sender.stop(wait=True, timeout=10)
        except Exception:
            pass

        # wait for control thread to exit (it was started non-daemon)
        try:
            if hasattr(self, "control_thread"):
                self.control_thread.join(timeout=5)
        except Exception:
            pass

    def start(self):
        self.sender.start()
        self.control_thread = threading.Thread(target = lambda: 
                            self.sum_control_consumer.start_consuming(self._control_callback), 
                            daemon=False
        )
        self.control_thread.start()
        self.input_queue.start_consuming(self._process_data_message)
        

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, lambda s, f: sum_filter._on_sigterm(s, f))
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
