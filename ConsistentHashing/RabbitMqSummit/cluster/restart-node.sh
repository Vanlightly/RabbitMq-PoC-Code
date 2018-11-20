#!/bin/bash

set -e

if [ $1 = "rabbitmq1" ]; then
    blockade restart rabbitmq1
    echo "rabbitmq1 restarted"
else
    # other nodes do not have rabbitmq-server as pid 1 and so stopping the container causes an unclean shutdown
    # therefore we do a controlled stop first
    R2_ID=$(blockade status | grep $1 | awk '{ print $2 }')    
    docker exec -it $R2_ID rabbitmqctl stop_app
    
    # restart the container
    blockade restart $2
    echo "$2 restarted"
fi
