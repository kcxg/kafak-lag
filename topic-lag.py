#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by 侃豺小哥 on 2019/7/11 16:36
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import TopicPartition

# https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
bootstrap_server = ['10.39.48.226:9092']

topic_list = ['data_iot_EMS',''

producer = KafkaProducer(bootstrap_servers=bootstrap_server)
consumer = KafkaConsumer(top,group_id='test_flink_stream_data_iot_EMS_dataclean',bootstrap_servers=bootstrap_server)
# 获取Lag值
def get_lag():
    partitions = producer.partitions_for(top)    #{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}
    sum = 0
    for pt in partitions:
        p = TopicPartition(topic=top, partition=pt)
        beginning_offsets = consumer.committed(p)
        end_offsets = consumer.end_offsets([p])
        sum = sum + end_offsets[p] - beginning_offsets
    return sum

if __name__ == '__main__':
    print(get_lag())




# test
# for message in consumer:
#     # message value and key are raw bytes -- decode if necessary!
#     # e.g., for unicode: `message.value.decode('utf-8')`
#     print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                                          message.offset, message.key,
#                                          message.value))
#
#     print("======")





