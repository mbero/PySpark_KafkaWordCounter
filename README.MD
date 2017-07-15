To execute this script properly you need to execute this as a one of parameters of spark submit file
inside your properly configured Spark instance

bin/spark-submit  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0  --jars spark-streaming-kafka-0-10_2.11-2.1.1.jar   examples/src/main/python/streaming/kafka_wordcount.py hdp-1.tap-psnc.net:2181 kafka-test