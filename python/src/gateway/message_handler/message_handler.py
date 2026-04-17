import uuid
import os
from common import message_protocol, middleware


MOM_HOST = os.environ["MOM_HOST"]


class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4())
        self.sum_control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, "sum_control", ["EOFs"])
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        self.sum_control_exchange.send(message_protocol.internal.serialize([self.client_id]))
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)

        if fields[0] == self.client_id:
            return fields[1]
        else:
            return None
