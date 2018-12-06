#!/bin/bash

blockade status | { while read line; \
do \
    node=$(echo $(echo $line | awk '{ print $1; }')); \
    if [[ $line == rabbit* ]] ; then \
        docker exec $node rabbitmq-plugins enable rabbitmq_consistent_hash_exchange; \
    fi; \
done; \

echo The Consistent Hash Exchange has been enabled on all nodes; }