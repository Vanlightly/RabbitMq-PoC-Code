#!/usr/bin/env python
import pika
from pika import spec
import sys
import subprocess
import requests
import json

def get_node_ip(node_name):
    bash_command = "bash ../cluster/get-node-ip.sh " + node_name
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    ip = output.decode('ascii').replace('\n', '')
    return ip

def put_ha_policy(mgmt_node_ip):
    r = requests.put('http://' + mgmt_node_ip + ':15672/api/policies/%2F/ha-queues', 
        data = "{\"pattern\":\"\", \"definition\": {\"ha-mode\":\"all\"}, \"priority\":0, \"apply-to\": \"queues\"}",
        auth=("jack","jack"))

    print(f"Create policy response: {r}")

exchange_name = sys.argv[1]
queue_prefix = sys.argv[2]
queue_count = int(sys.argv[3])

node_ip = get_node_ip("rabbitmq1")
put_ha_policy(node_ip)

credentials = pika.PlainCredentials('jack', 'jack')
parameters = pika.ConnectionParameters(node_ip,
                                    5672,
                                    '/',
                                    credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.exchange_declare(exchange=exchange_name, exchange_type='x-consistent-hash', durable=True)
print(f"Declared exchange {exchange_name}")

for i in range(1, queue_count+1):
    suffix = f"{i:03}"
    queue_name = f"{queue_prefix}{suffix}"
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_purge(queue_name)
    channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key="10")
    print(f"Declared and bound queue {queue_name}")

