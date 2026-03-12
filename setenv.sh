#!/bin/sh


### JAR files for spark ###

JAR_DIR="spark_jars"
if [ ! -d "./$JAR_DIR" ]; then
    mkdir "$JAR_DIR"
fi

MAVEN_URL="https://repo1.maven.org/maven2"
HADOOP_JAR="hadoop-aws-3.4.1.jar"
WILDFLY_JAR="wildfly-openssl-1.1.3.Final.jar"
BUNDLE_JAR="bundle-2.24.6.jar"

if [ ! -f "./$JAR_DIR/$HADOOP_JAR" ]; then
    wget "$MAVEN_URL/org/apache/hadoop/hadoop-aws/3.4.1/$HADOOP_JAR" -P "$JAR_DIR"
fi

if [ ! -f "./$JAR_DIR/$WILDFLY_JAR" ]; then
    wget "$MAVEN_URL/org/wildfly/openssl/wildfly-openssl/1.1.3.Final/$WILDFLY_JAR" -P "$JAR_DIR"
fi

if [ ! -f "./$JAR_DIR/$BUNDLE_JAR" ]; then
    wget "$MAVEN_URL/software/amazon/awssdk/bundle/2.24.6/$BUNDLE_JAR" -P "$JAR_DIR"
fi


### Environment variables ###

export PD2_DATA_PATH="$PWD/data"
export PD2_CLEAN_PATH="$PD2_DATA_PATH/clean"
export PD2_MERGED_PATH="$PD2_DATA_PATH/merged"
export PD2_AGG_PATH="$PD2_DATA_PATH/prepared_for_model"
