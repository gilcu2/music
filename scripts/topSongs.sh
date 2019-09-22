#!/usr/bin/env bash

MASTER="local[*]"
CONFIGPATH="."
PROGRAM="target/scala-2.11/Music.jar"
MAIN=com.gilcu2.MusicMain
NAME=topSongs
OUT=$NAME.out
ERR=$NAME.err
if [[ $DEBUG ]];then
    export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
fi

spark-submit \
--class $MAIN \
--master $MASTER \
--conf "spark.driver.extraClassPath=$CONFIGPATH" \
--executor-memory 6G  \
--driver-memory 4G \
$PROGRAM "$@" 2>$ERR |tee $OUT

echo Output is in $OUT, error output in $ERR
