#!/usr/bin/env bash

MASTER="local[8]"
CONFIGPATH="."
PROGRAM="../target/scala-2.11/SparkStreaming.jar"
MAIN=com.gilcu2.KafkaStreamingMain
OUT=kafka.out
ERR=kafka.err
if [[ $DEBUG ]];then
    export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
fi

spark-submit \
--class $MAIN \
--master $MASTER \
--conf "spark.driver.extraClassPath=$CONFIGPATH" \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3,org.apache.kafka:kafka-clients:0.10.2.2 \
$PROGRAM "$@" 2>$ERR |tee $OUT

echo Output is in $OUT, error output in $ERR
