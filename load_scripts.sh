#!/usr/bin/env bash

for i in fac19node1 
do
scp -i ~/.ssh/az_id_rsa  /home/ubuntu/uppaal64-4.1.19.zip ubuntu@${i}:/home/ubuntu/
scp -i ~/.ssh/az_id_rsa  /home/ubuntu/xSpark-bench/zot_setup.sh ubuntu@${i}:/home/ubuntu/
ssh -i ~/.ssh/az_id_rsa ubuntu@${i} sh zot_setup.sh
done