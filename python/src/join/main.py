import os
import logging

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

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        try:
            fields = message_protocol.internal.deserialize(message)
        except Exception:
            logging.exception("Failed to deserialize message")
            nack()
            return
        
        if len(fields) != 2:
            logging.warning(f"Unexpected message format: {message}")
            nack()
            return
        
        client_id, fruit_top = fields

        # Send result including client_id
        try:
            self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))
            logging.info("Forwarded top for client %s", client_id)
            ack()
        except Exception:
            logging.exception("Failed to forward top for client %s", client_id)
            nack()
            return

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
