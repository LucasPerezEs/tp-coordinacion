import pika
import random
import string

import pika.exceptions
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            self.queue_name = queue_name
            self.channel.queue_declare(queue=queue_name)
        except Exception:
            if self.connection.is_open:
                self.connection.close()

    
    def start_consuming(self, on_message_callback):
        def callback_wrapper(ch, method, properties, body):
            def ack(): ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack(): ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback_wrapper, auto_ack=False)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        except Exception:
            raise MessageMiddlewareMessageError

    def stop_consuming(self):
        try:
            if self.channel.is_open:
                self.channel.stop_consuming()

        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
            

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
            )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        
        except pika.exceptions.ChannelWrongStateError:
            raise MessageMiddlewareDisconnectedError
        
        except Exception:
            raise MessageMiddlewareMessageError


    def close(self):
        try:
            self.connection.close()
        except:
            raise MessageMiddlewareCloseError

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()

            self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
            self.exchange_name = exchange_name

            result = self.channel.queue_declare(queue='', exclusive=True)
            self.queue_name = result.method.queue

            self.routing_keys = routing_keys

            for key in routing_keys:
                self.channel.queue_bind(self.queue_name, exchange_name, key)
                

        except Exception:
            if self.connection.is_open:
                self.connection.close()
            raise
    
    def start_consuming(self, on_message_callback):
        def callback_wrapper(ch, method, properties, body):
            def ack(): ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack(): ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)
        
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback_wrapper, auto_ack=False)
            self.channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        
        except Exception:
            raise MessageMiddlewareMessageError

    def stop_consuming(self):
        try:
            if self.channel.is_open:
                self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=self.routing_keys[0],
                body=message,
            )

        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        
        except pika.exceptions.ChannelWrongStateError:
            raise MessageMiddlewareDisconnectedError
        
        except Exception:
            raise MessageMiddlewareMessageError

    def close(self):
        try:
            if self.connection.is_open:
                self.connection.close()
        except:
            raise MessageMiddlewareCloseError
