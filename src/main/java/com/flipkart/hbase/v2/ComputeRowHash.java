package com.flipkart.hbase.v2;

import com.google.common.base.Splitter;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

public class ComputeRowHash {

    private static final String NAME = "computerowhash";

    public enum Counters {ROWS, ERROR, SKIP, OUT_OF_TIME_RANGE}

    public static class ComputeHash
            extends TableMapper<Text, Text> {

        @Override
        public void map(ImmutableBytesWritable row, final Result value, Context context) {

            context.getCounter(Counters.ROWS).increment(1);

            final Hasher hasher = Hashing.murmur3_128().newHasher();
            long timestamp = 0;

            try {
                timestamp = value.rawCells()[0].getTimestamp();
            } catch (Exception e) {
                // ignore
            }

            if (timestamp == 0) {
                context.getCounter(Counters.SKIP).increment(1);
                return;
            }
            final long startTime = context.getConfiguration().getLong("start", 0);
            final long endTime = context.getConfiguration().getLong("end", 0);

            if (!(timestamp >= startTime && timestamp < endTime)) {
                context.getCounter(Counters.OUT_OF_TIME_RANGE).increment(1);
                return;
            }

            for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> familyMap : value.getNoVersionMap().entrySet()) {
                for (final Map.Entry<byte[], byte[]> qv : familyMap.getValue().entrySet()) {
                    hasher.putBytes(row.copyBytes())
                            .putBytes(familyMap.getKey())
                            .putBytes(qv.getKey())
                            .putBytes(qv.getValue());
                }
            }

            final HashCode hashCode = hasher.hash();
            try {
                context.write(new Text(row.copyBytes()), new Text(String.format("%d %s", timestamp, hashCode.toString())));
            } catch (IOException e) {
                context.getCounter(Counters.ERROR).increment(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static class BuildReport extends Reducer<Text, Text, Text, Text> {

    }

    public static Job createSubmittableJob(final Configuration conf, final String[] args) throws IOException {

        final Scan scan = new Scan();
        scan.setCacheBlocks(false);

        final String tableName = args[0];
        final String cfs = args[1];
        final long startTime = Long.parseLong(args[2]);
        final long endTime = Long.parseLong(args[3]);
        final String outputFilePath = args[4];

        conf.setLong("start", startTime);
        conf.setLong("end", endTime);
        for (final String cf : Splitter.on(",").omitEmptyStrings().split(cfs)) {
            final byte[] family = Bytes.toBytes(cf);
            scan.addFamily(family);
        }


        Job job = new Job(conf, NAME + "_" + tableName);
        job.setJarByClass(ComputeRowHash.class);
        job.setSpeculativeExecution(false);
        job.setMapSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);

        TableMapReduceUtil.initTableMapperJob(tableName, scan,
                ComputeHash.class, Text.class, Text.class, job);
        job.setReducerClass(BuildReport.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath));

        return job;
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 5) {
            System.err.println("Usage: " + NAME + " <table-name> <column-families> <start-time> <end-time> <output-file>");
            System.exit(1);
        }

        final Configuration conf = HBaseConfiguration.create();
        final Job job = createSubmittableJob(conf, args);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
