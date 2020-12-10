#!/bin/bash
apt update
apt install -y openjdk-11-jre openjdk-11-jdk awscli tesseract-ocr
aws s3 cp s3://dsp-ass1-na/WorkerWithTesseract.jar /home/ubuntu/Worker.jar
java -jar /home/ubuntu/Worker.jar


#!/bin/bash
apt update
apt install -y openjdk-11-jre openjdk-11-jdk awscli
aws s3 cp s3://dsp-ass1-na/Worker.jar /home/ubuntu/Worker.jar
aws s3 cp s3://dsp-ass1-na/Tess4J.tar /home/ubuntu/Tess4J.tar
tar xf Tess4J.tar