#!/bin/bash

docker-compose run --rm app bash -c "cd /home/code/; npm install"
docker-compose run --rm app bash -c "cd /home/code; npm test test/*.js"
