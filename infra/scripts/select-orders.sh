#!/bin/sh

docker exec -it postgres psql -h 127.0.0.1 -U postgres -d postgres -c 'select * from orders';
