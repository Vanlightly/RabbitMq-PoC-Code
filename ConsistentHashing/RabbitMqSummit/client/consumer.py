#!/usr/bin/env python
import pika
from pika import spec
import time
import datetime
import sys
import random
import subprocess
from command_args import get_args, get_mandatory_arg, get_optional_arg


class RabbitConsumer:
    connection = None
    receive_channel = None
    publish_channel = None
    queue_name = ""
    out_queue_name = ""
    processing_ms_min = 0
    processing_ms_max = 0
    # in a production system you would need a data structure that could expire items over time
    history = set() 
    last_msg_time = datetime.datetime.now()
    dedup_enabled = False

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
        # probably I am running a different demo without restarting this consumer
        if (datetime.datetime.now() - self.last_msg_time).seconds > 5:
            self.history.clear()
            
        if self.dedup_enabled and properties.correlation_id in self.history:
            print("Detected and ignored duplicate")
            ch.basic_ack(delivery_tag = method.delivery_tag)
        else:
            self.history.add(properties.correlation_id)
            self.publish_channel.basic_publish(exchange='', 
                                                routing_key=self.out_queue_name,
                                                body=body)   

            ch.basic_ack(delivery_tag = method.delivery_tag)

            if self.processing_ms_max > 0:
                wait_sec = float(random.randint(self.processing_ms_min, self.processing_ms_max) / 1000)
                time.sleep(wait_sec)
        
        self.last_msg_time = datetime.datetime.now()

    def consume(self, queue, out_queue, prefetch, processing_ms_min, processing_ms_max, dedup_enabled):
        self.queue_name = queue
        self.out_queue_name = out_queue
        self.dedup_enabled = dedup_enabled
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

args = get_args(sys.argv)

connect_node = get_optional_arg(args, "--node", "rabbitmq1") 
queue = get_mandatory_arg(args, "--in-queue") 
out_queue = get_mandatory_arg(args, "--out-queue") 
prefetch =  int(get_optional_arg(args, "--prefetch", "1"))
processing_ms_min = int(get_optional_arg(args, "--min-ms", "0")) 
processing_ms_max = int(get_optional_arg(args, "--max-ms", "0")) 
dedup_enabled = get_optional_arg(args, "--dedup", "false") == "true"

print(f"Consuming queue: {queue} Writing to: {out_queue}")

consumer = RabbitConsumer()
consumer.connect(connect_node)
consumer.consume(queue, out_queue, prefetch, processing_ms_min, processing_ms_max, dedup_enabled)
