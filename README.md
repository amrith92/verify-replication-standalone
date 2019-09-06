# verify-replication-standalone
Standalone replication verifier for HBase 0.94, CDH 4.7.0 distribution

## Building the project

This project requires Java 8.


    ./gradlew clean build


This results in a JAR being built (dependency-free) in the `build/libs` directory.

## Running the Verify MR job

The source HBase cluster must have MapReduce installed.

On the source clusters' HMaster, run:

    HADOOP_CLASSPATH=`hbase classpath` hadoop jar verify-replication-standalone-1.0-SNAPSHOT.jar com.flipkart.hbase.v094.cdh470.replication.VerifyReplicationCutover --starttime=<start-time> --stoptime=<stop-time> --out=<output-file-path> <peer-id> <table-name>
    
