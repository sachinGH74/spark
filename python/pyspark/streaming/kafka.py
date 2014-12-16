#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from py4j.java_collections import MapConverter
from py4j.java_gateway import java_import, Py4JError

from pyspark.storagelevel import StorageLevel
from pyspark.serializers import PairDeserializer, UTF8Deserializer
from pyspark.streaming import DStream

__all__ = ['KafkaUtils']


class KafkaUtils(object):

    @staticmethod
    def createStream(ssc, zkQuorum, groupId, topics,
                     storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2,
                     keyDecoder=None, valueDecoder=None):
        """
        Create an input stream that pulls messages from a Kafka Broker.

        :param ssc:  StreamingContext object
        :param zkQuorum:  Zookeeper quorum (hostname:port,hostname:port,..).
        :param groupId:  The group id for this consumer.
        :param topics:  Dict of (topic_name -> numPartitions) to consume.
                        Each partition is consumed in its own thread.
        :param storageLevel:  RDD storage level.
        :param keyDecoder:  A function used to decode key
        :param valueDecoder:  A function used to decode value
        :return: A DStream object
        """
        java_import(ssc._jvm, "org.apache.spark.streaming.kafka.KafkaUtils")

        if not isinstance(topics, dict):
            raise TypeError("topics should be dict")
        jtopics = MapConverter().convert(topics, ssc.sparkContext._gateway._gateway_client)
        jlevel = ssc._sc._getJavaStorageLevel(storageLevel)
        try:
            jstream = ssc._jvm.KafkaUtils.createStream(ssc._jssc, zkQuorum, groupId, jtopics,
                                                       jlevel)
        except Py4JError, e:
            if 'call a package' in e.message:
                print "No kafka package, please build it and add it into classpath:"
                print " $ sbt/sbt streaming-kafka/package"
                print " $ bin/submit --driver-class-path external/kafka/target/scala-2.10/" \
                      "spark-streaming-kafka_2.10-1.3.0-SNAPSHOT.jar"
                raise Exception("No kafka package")
            raise e
        ser = PairDeserializer(UTF8Deserializer(), UTF8Deserializer())
        stream = DStream(jstream, ssc, ser)

        if keyDecoder is not None:
            stream = stream.map(lambda (k, v): (keyDecoder(k), v))
        if valueDecoder is not None:
            stream = stream.mapValues(valueDecoder)
        return stream
