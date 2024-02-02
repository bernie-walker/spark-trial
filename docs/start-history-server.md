1. Set these configurations in $SPARK_HOME/conf/spark-defaults.conf
    ```
    spark.eventLog.enabled           true
    spark.eventLog.dir               <eventLogsDir>
    spark.history.fs.logDirectory    <eventLogsDir>
    ```
> The value of **eventLogsDir** could be local Fs, HDFS or any other hadoop compatible file systems(eg: s3://, abfs:// etc). Checkout **Hadoop Compatible File Systems** in hadoop docs for configuring these.
2. Execute the command `$SPARK_HOME/sbin/start-history-server.sh`