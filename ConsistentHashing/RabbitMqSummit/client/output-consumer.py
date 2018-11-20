#!/usr/bin/env python
import pika
import sys
import time
import subprocess
import datetime
import threading
from command_args import get_args, get_mandatory_arg, get_optional_arg

def get_node_ip(node_name):
    bash_command = "bash ../cluster/get-node-ip.sh " + node_name
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    ip = output.decode('ascii').replace('\n', '')
    return ip

def monitor():
    global keys, last_msg_time

    while(True):
        # probably I am running a different demo without restarting this consumer
        if len(keys) > 0 and (datetime.datetime.now() - last_msg_time).seconds > 2:
            final_state = ""
            for key, value in keys.items():
                final_state += key + "=" + str(value) + " "
            print(final_state)
            keys.clear()
            history.clear()
            print("----------------------------------")
        time.sleep(1)

def callback(ch, method, properties, body):
    global keys, last_msg_time

    # # probably I am running a different demo without restarting this consumer
    # if (datetime.datetime.now() - last_msg_time).seconds > 2:
    #     final_state = ""
    #     for key, value in keys:
    #         final_state += key + "=" + value + " "
    #     print(final_state)
    #     keys.clear()
    #     history.clear()
    #     print("----------------------------------")

    body_str = str(body, "utf-8")
    parts = body_str.split('=')
    key = parts[0]
    curr_value = int(parts[1])

    duplicate = ""
    if body_str in history:
        duplicate = " DUPLICATE"
    history.add(body_str)

    if key in keys:
        last_value = keys[key]
        
        if last_value + 1 < curr_value:
            jump = curr_value - last_value
            print(f"{body} Jump forward {jump} {duplicate}")
        elif last_value > curr_value:
            jump = last_value - curr_value
            print(f"{body} Jump back {jump} {duplicate}")
        else:
            print(f"{body} {duplicate}")
    else:
        if curr_value == 1:
            print(f"{body} {duplicate}")
        else:
            print(f"{body} Jump forward {curr_value} {duplicate}")
        
    keys[key] = curr_value

    ch.basic_ack(delivery_tag = method.delivery_tag)
    last_msg_time = datetime.datetime.now()

args = get_args(sys.argv)
connect_node = get_optional_arg(args, "--node", "rabbitmq1") #sys.argv[1]
ip = get_node_ip(connect_node)

queue = get_mandatory_arg(args, "--queue")

keys = dict()
history = set()
last_msg_time = datetime.datetime.now()

monitor_thread = threading.Thread(target=monitor)
monitor_thread.start()

credentials = pika.PlainCredentials('jack', 'jack')
parameters = pika.ConnectionParameters(ip,
                                    5672,
                                    '/',
                                    credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue=queue)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    connection.close()
except Exception as ex:
    template = "An exception of type {0} occurred. Arguments:{1!r}"
    message = template.format(type(ex).__name__, ex.args)
    print(message) 
    connection.close()