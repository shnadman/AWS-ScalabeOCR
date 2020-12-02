#!/bin/bash
apt update
apt install -y openjdk-11-jre openjdk-11-jdk awscli
aws s3 cp s3://dsp-ass1-na/Worker.jar /home/ubuntu/Worker.jar
java -jar /home/ubuntu/Worker.jar
