#!/bin/bash

docker-compose run --rm app bash -c "cd /home/code; npm install; npm test test/*.js"
