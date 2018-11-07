#!/usr/bin/env python
import pika
from pika import spec
import sys
import time
import subprocess
import datetime

connect_node = sys.argv[1]
node_count = int(sys.argv[2])
count = int(sys.argv[3])
queue = sys.argv[4]

terminate = False
exit_triggered = False
last_ack_time = datetime.datetime.now()
last_ack = 0

node_names = []

curr_pos = 0
pending_messages = list()
pending_acks = list()
pos_acks = 0
neg_acks = 0

for i in range(1, node_count+1):
    node_names.append(f"rabbitmq{i}")
nodes = list()

def get_node_index(node_name):
    index = 0
    for node in node_names:
        if node == node_name:
            return index

        index +=1

    return -1

def on_open(connection):
    connection.channel(on_channel_open)
    print("Connection open")

def on_channel_open(chan):
    global connection, channel
    chan.confirm_delivery(on_delivery_confirmation)
    channel = chan
    publish_messages()

# this is ignoring the posibility of ack + return
# do not use in production code
def on_delivery_confirmation(frame):
    global last_ack_time, pending_messages, pos_acks, neg_acks, last_ack, count

    if isinstance(frame.method, spec.Basic.Ack) or isinstance(frame.method, spec.Basic.Nack):
        if frame.method.multiple == True:
            acks = 0
            messages_to_remove = [item for item in pending_messages if item <= frame.method.delivery_tag]
            for val in messages_to_remove:
                try:
                    pending_messages.remove(val)
                except:
                    print(f"Could not remove multiple flag message: {val}")
                acks += 1
        else:
            try:
                pending_messages.remove(frame.method.delivery_tag) 
            except:
                print(f"Could not remove non-multiple flag message: {frame.method.delivery_tag}")
            acks = 1

    if isinstance(frame.method, spec.Basic.Ack):
        pos_acks += acks
    elif isinstance(frame.method, spec.Basic.Nack):
        neg_acks += acks
    elif isinstance(frame.method, spec.Basic.Return):
        print("Undeliverable message")
    

    curr_ack = int((pos_acks + neg_acks) / 10000)
    if curr_ack > last_ack:
        print(f"Pos acks: {pos_acks} Neg acks: {neg_acks}")
        last_ack = curr_ack

    if (pos_acks + neg_acks) >= count:
        print(f"Final Count => Pos acks: {pos_acks} Neg acks: {neg_acks}")
        connection.close()
        exit(0)

def publish_messages():
    global connection, channel, queue, count, pending_messages, curr_pos, state_index, val

    while curr_pos < count:
        if channel.is_open:
            curr_pos += 1
            body = f"{curr_pos}"
            channel.basic_publish(exchange='', 
                                routing_key=queue,
                                body=body,
                                properties=pika.BasicProperties(content_type='text/plain',
                                                        delivery_mode=2))

            pending_messages.append(curr_pos)

            if curr_pos % 1000 == 0:
                if len(pending_messages) > 10000:
                    #print("Reached in-flight limit, pausing publishing for 2 seconds")
                    if channel.is_open:
                        connection.add_timeout(2, publish_messages)
                        break
            
        else:
            print("Channel closed, ceasing publishing")
            break

def on_close(connection, reason_code, reason_text):
    connection.ioloop.stop()
    print("Connection closed. Reason: " + reason_text)

def reconnect():
    print("Reconnect called")
    global curr_node
    curr_node += 1
    if curr_node > 2:
        print("Failed to connect. Will retry in 5 seconds")
        time.sleep(5)
        curr_node = 0
    
    connect()

def connect():
    global connection, curr_node, terminate
    print("Attempting to connect to " + nodes[curr_node])
    parameters = pika.URLParameters('amqp://jack:jack@' + nodes[curr_node] + ':5672/%2F')
    connection = pika.SelectConnection(parameters=parameters,
                                on_open_callback=on_open,
                                on_open_error_callback=reconnect,
                                on_close_callback=on_close)

    try:
        connection.ioloop.start()
    except KeyboardInterrupt:
        connection.close()
        connection.ioloop.stop()
        terminate = True
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)

    print("Disconnected")

def get_node_ip(node_name):
    bash_command = "bash ../cluster/get-node-ip.sh " + node_name
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    ip = output.decode('ascii').replace('\n', '')
    return ip

for node_name in node_names:
    nodes.append(get_node_ip(node_name))

curr_node = get_node_index(connect_node)

# keep running until the terminate signal has been received
while terminate == False:
    try:
        connect()
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)

        if terminate == False:
            reconnect()
