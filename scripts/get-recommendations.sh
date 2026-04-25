#!/bin/sh

curl -s http://localhost:8080/recommendations | jq .
