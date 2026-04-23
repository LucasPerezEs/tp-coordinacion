import os
import logging
import signal
import threading

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.shutdown = threading.Event()
        self.lock = threading.Lock()
    
        self.partials_by_client = {} # {client_id: {agg_id: partial_top}}
        self.callbacks_by_client = {} # client_id -> {agg_id: (ack, nack)}

        self.sending_clients = set()
        self.sent_clients = set()


    def process_messsage(self, message, ack, nack):
        logging.info("Received partial/top from aggregator")
        try:
            fields = message_protocol.internal.deserialize(message)
        except Exception:
            logging.exception("Failed to deserialize message")
            nack()
            return
        
        if len(fields) != 3:
            logging.warning("Unexpected message format: %s", fields)
            nack()
            return
        
        client_id, agg_id, fruit_top = fields

        # Basic validations
        if client_id is None or agg_id is None:
            logging.warning("Missing client_id or agg_id: %s", fields)
            nack()
            return

        # Store partial and its ack/nack for later acking after final send
        staged_partials = None
        staged_callbacks = None
        with self.lock:
            # If already finished for this client, discard duplicate partial
            if client_id in self.sent_clients:
                logging.info("Received partial for already-sent client %s, discarding", client_id)
                try:
                    ack()
                except Exception:
                    pass
                return
            
            # Initialize structures
            client_partials = self.partials_by_client.setdefault(client_id, {})
            client_callbacks = self.callbacks_by_client.setdefault(client_id, {})

            # Deduplicate by agg_id
            if agg_id in client_partials:
                logging.info("Duplicate partial from agg %s for client %s, ignoring", agg_id, client_id)
                try:
                    ack()
                except Exception:
                    pass
                return
            
            # Store the partial and callbacks (do not ack now)
            client_partials[agg_id] = fruit_top
            client_callbacks[agg_id] = (ack, nack)

            # If we don't have all AGGREGATION_AMOUNT partials yet, return (no ack)
            if len(client_partials) < AGGREGATION_AMOUNT:
                logging.info("Stored partial for client %s (%d/%d)", client_id, len(client_partials), AGGREGATION_AMOUNT)
                return
        
            # We have all partials: stage them for sending and remove from active maps
            staged_partials = self.partials_by_client.pop(client_id)
            staged_callbacks = self.callbacks_by_client.pop(client_id)
            self.sending_clients.add(client_id)

        try:
            merged = {}
            for p in staged_partials.values():
                if not p:
                    continue
                for fruit, amt in p:
                    merged[fruit] = merged.get(fruit, 0) + int(amt)

            # build top
            items = sorted([ (f,a) for f,a in merged.items() ], key=lambda x: x[1], reverse=True)
            final_top = items[:TOP_SIZE]

            # send to Gateway (blocking / reliable). Do not ack source partials until send confirmed.
            self.output_queue.send(message_protocol.internal.serialize([client_id, final_top]))
            logging.info("Forwarded final top for client %s: %s", client_id, final_top)

        except Exception:
            logging.exception("Failed to forward final top for client %s, will restore partials and nack", client_id)
            # restore staged partials and requeue (nack) the original partial messages
            with self.lock:
                # restore maps if needed
                existing = self.partials_by_client.setdefault(client_id, {})
                existing_callbacks = self.callbacks_by_client.setdefault(client_id, {})
                for aid, part in staged_partials.items():
                    existing[aid] = part
                    existing_callbacks[aid] = staged_callbacks[aid]
                self.sending_clients.discard(client_id)

            # nack each original partial so broker will redeliver
            for aid, cb in staged_callbacks.items():
                try:
                    cb[1]()  # call nack
                except Exception:
                    pass
            return

        # success: ack all original partial messages and mark as sent
        with self.lock:
            self.sending_clients.discard(client_id)
            self.sent_clients.add(client_id)

        for aid, cb in staged_callbacks.items():
            try:
                cb[0]()  # call ack
            except Exception:
                pass


    def _on_sigterm(self, signum, frame):
        logging.info("SIGTERM received, shutting down joiner")
        self.shutdown.set()
        try:
            self.input_queue.stop_consuming()
        except Exception:
            pass

    def _cleanup(self):
        try: 
            self.output_queue.close()
        except: 
            pass
        try: 
            self.input_queue.close()
        except: 
            pass


    def start(self):
        signal.signal(signal.SIGTERM, lambda s,f: self._on_sigterm(s,f))
        try:
            self.input_queue.start_consuming(self.process_messsage)
        finally:
            self._cleanup()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
