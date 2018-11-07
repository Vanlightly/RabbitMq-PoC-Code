#!/usr/bin/env python
import pika
from pika import spec
import time
import sys
import random
import subprocess


class RabbitConsumer:
    connection = None
    receive_channel = None
    publish_channel = None
    queue_name = ""
    processing_ms_min = 0
    processing_ms_max = 0

    def get_node_ip(self, node_name):
        bash_command = "bash ../cluster/get-node-ip.sh " + node_name
        process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        ip = output.decode('ascii').replace('\n', '')
        return ip

    def connect(self, node):
        ip = self.get_node_ip(node)
        credentials = pika.PlainCredentials('jack', 'jack')
        parameters = pika.ConnectionParameters(ip,
                                            5672,
                                            '/',
                                            credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.receive_channel = self.connection.channel()
        self.publish_channel = self.connection.channel()

    def callback(self, ch, method, properties, body):
        
        self.publish_channel.basic_publish(exchange='', 
                                            routing_key='output',
                                            body=body)   

        ch.basic_ack(delivery_tag = method.delivery_tag)

        if self.processing_ms_max > 0:
            wait_sec = float(random.randint(self.processing_ms_min, self.processing_ms_max) / 1000)
            time.sleep(wait_sec)

    def consume(self, queue, prefetch, processing_ms_min, processing_ms_max):
        self.queue_name = queue
        print(f"Consuming queue: {self.queue_name}")
        self.receive_channel.basic_qos(prefetch_count=prefetch)
        self.receive_channel.basic_consume(self.callback,
                      queue=self.queue_name,
                      no_ack=False)

        self.processing_ms_min = processing_ms_min
        self.processing_ms_max = processing_ms_max

        try:
            self.receive_channel.start_consuming()
        except KeyboardInterrupt:
            self.disconnect()
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message) 
                    
    def disconnect(self):
        self.connection.close()

connect_node = sys.argv[1]
queue = sys.argv[2]
prefetch = int(sys.argv[3])
processing_ms_min = int(sys.argv[4])
processing_ms_max = int(sys.argv[5])
consumer = RabbitConsumer()
consumer.connect(connect_node)
consumer.consume(queue, prefetch, processing_ms_min, processing_ms_max)
