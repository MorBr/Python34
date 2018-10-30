#!/usr/bin/env python
import pika
import sys

##################
# queue initialize
##################
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='testQu')

##############################
# get message input & publish
##############################
if len(sys.argv[1:]) < 1:
    print('not valid input parameters')
    exit()
    connection.close()
else:
    message = ' '.join(sys.argv[1:])
    channel.basic_publish(exchange='',
                          routing_key='testQu',
                          body=message,
                       )
    print(" [x] Sent %r" % message)
    connection.close()