#!/bin/bash
docker network create mr_network
chmod +x configure_network.py
./configure_network.py "mr_network"
echo "Network mr_network has been added to mr-data-node/docker-compose.yaml if not existed yet!"
docker-compose up --build --remove-orphans