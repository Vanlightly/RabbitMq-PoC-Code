#!/usr/bin/env python
import pika
import sys
import time
import subprocess
import datetime

def get_node_ip(node_name):
    bash_command = "bash ../cluster/get-node-ip.sh " + node_name
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    ip = output.decode('ascii').replace('\n', '')
    return ip

def callback(ch, method, properties, body):
    global keys, last_msg_time

    # probably I am running a different demo without restarting this consumer
    if (datetime.datetime.now() - last_msg_time).seconds > 5:
        keys.clear()
        print("----------------------------------")

    parts = str(body, "utf-8").split('=')
    key = parts[0]
    curr_value = int(parts[1])

    if key in keys:
        last_value = keys[key]
        
        if last_value + 1 < curr_value:
            jump = curr_value - last_value
            print(f"{body} Jump forward {jump}")
        elif last_value > curr_value:
            jump = last_value - curr_value
            print(f"{body} Jump back {jump}")
        else:
            print(body)
    else:
        if curr_value == 0:
            print(body)
        else:
            print(f"{body} Jump forward {curr_value}")
    
    keys[key] = curr_value

    ch.basic_ack(delivery_tag = method.delivery_tag)
    last_msg_time = datetime.datetime.now()

connect_node = sys.argv[1]
ip = get_node_ip(connect_node)

keys = dict()
last_msg_time = datetime.datetime.now()

credentials = pika.PlainCredentials('jack', 'jack')
parameters = pika.ConnectionParameters(ip,
                                    5672,
                                    '/',
                                    credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='output')

try:
    channel.start_consuming()
except KeyboardInterrupt:
    connection.close()
except Exception as ex:
    template = "An exception of type {0} occurred. Arguments:{1!r}"
    message = template.format(type(ex).__name__, ex.args)
    print(message) 
    connection.close()