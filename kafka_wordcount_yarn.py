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

"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 Usage: kafka_wordcount.py <zk> <topic>

 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      examples/src/main/python/streaming/kafka_wordcount.py \
      localhost:2181 test`
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import os
import sys
import random
from operator import add

# Configure the environment
if 'SPARK_HOME' not in os.environ:
    print("no spark home")
    os.environ['SPARK_HOME'] = '/usr/hdp/current/spark2-client/'

# Create a variable for our root path
SPARK_HOME = os.environ['SPARK_HOME']
print("---spark home is---" + SPARK_HOME)

sys.path.insert(0, os.path.join(SPARK_HOME, "python", "build"))
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))

import findspark

findspark.init()

from pyspark import SparkConf

# local spark
conf = SparkConf()
# conf.setMaster('spark://10.200.132.190' + ':7077')

conf.setMaster('local[*]')
conf.setAppName("a-test_app")
conf.set("spark.shuffle.service.enabled", "false")
conf.set("spark.dynamicAllocation.enabled", "false")
conf.set("spark.executor.memory", "512m")
conf.set("spark.python.worker.memory", "512m")
conf.set("spark.executor.cores", "2")
conf.set("spark.cores.max", "2")
conf.set("spark.python.cores.max", "2")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

        # sc = SparkContext(appName="PythonStreamingKafkaWordCount")
        # ssc = StreamingContext(sc, 1)
        sc = SparkContext.getOrCreate(conf=conf)
        sc.stop()
        sc = SparkContext(conf=conf)
        ssc = StreamingContext(sc, 1)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
