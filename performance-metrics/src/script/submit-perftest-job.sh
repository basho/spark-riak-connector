#!/bin/bash
set -e
#set -x

function usage()
{
    echo "usage:"
    echo "./submit-performance-test-job.sh"
    echo "    -h --help"
    echo "    --jar  [path/to/jarfile]"
    echo "    --class  [full class name]"
    echo "    --master [spark master uri]"
    echo "    --args [jar arguments]"
    echo ""
}

while [[ $# > 1 ]]
do
key="$1"

    case $key in
        -h | --help)
            usage
            exit 0
            ;;
        --jar)
            JAR="$2"
            shift # past argument
            ;;
        --class)
            JAVA_CLASS="$2"
            shift # past argument
            ;;
        --master)
            SPARK_MASTER="$2"
            shift # past argument
            ;;
        --args)
            JAR_ARGS="$2"
            shift # past argument
            ;;
        *)
            echo "ERROR: unknown parameter \"$1\""
            usage
            exit 1
            ;;
    esac
    shift
done

[ -z "$JAR" ] && JAR="/home/ubuntu/performance-tests/performance-tests.jar"
[ -z "$JAVA_CLASS" ] && JAVA_CLASS="com.basho.spark.connector.perf.FullBucketReadPerformanceApp"
#[ -z "$SPARK_MASTER" ] && SPARK_MASTER="spark://ip-172-31-57-179:7077"
[ -z "$JAR_ARGS" ] && JAR_ARGS="/home/ubuntu/performance-tests/perf-tests.config"
[ -z "$SPARK_HOME" ] && SPARK_HOME="/opt/spark"

if [ ! -f $JAR ]; then
    echo "Jar file $JAR not found!"
    exit 1
fi

echo "Runnnign Spark performance test:"
echo "    Jar file $JAR"
echo "    Java class $JAVA_CLASS"
echo "    Spark master $SPARK_MASTER"
echo "    Jar argumets $JAR_ARGS"

# --master $SPARK_MASTER
spark_submit="$SPARK_HOME/bin/spark-submit --class $JAVA_CLASS $JAR $JAR_ARGS"

if [ -f master_stdout.log ]; then
    rm master_stdout.log
fi

echo
echo "Submiting job to spark"
echo "cmd -> $spark_submit"

$spark_submit &> ./master_stdout.log

config=$(sed -n "/Job configuration/,/End of job configuration/p" master_stdout.log)
echo "$config"
appId=$(echo "$config" | grep "spark.app.id" | awk -F"= " '{ print $2 }')

echo "Spark job finished. Gathering performance logs..."

./collect-perf4j-logs.sh "$config" "$appId"
