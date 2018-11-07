#!/bin/bash

cd ../cluster

if blockade status > /dev/null 2>&1; then
    echo Destroying blockade cluster
    blockade destroy
fi

cp ./blockade-files/blockade-$1nodes.yml blockade.yml

echo Creating blockade cluster
if ! blockade up > /dev/null 2>&1; then
    echo Blockade error, aborting test
    exit 1
fi

blockade status

echo "enabling Consistent Hash Exchange on all nodes..."

bash enable-c-hash-ex.sh
