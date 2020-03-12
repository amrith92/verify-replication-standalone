package com.flipkart.hbase.v094.cdh470;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.hash.MurmurHash;

import java.io.IOException;

public class ComputeRowHash {

    private static final String NAME = "computerowhash";

    public enum Counters {ROWS,ERROR}

    public static class ComputeHash
            extends TableMapper<ImmutableBytesWritable, Text> {

        @Override
        public void map(ImmutableBytesWritable row, final Result value, Context context) {

            context.getCounter(Counters.ROWS).increment(1);

            final int hash = MurmurHash.getInstance().hash(value.getBytes().get());
            try {
                context.write(row, new Text(Integer.toString(hash)));
            } catch (IOException e) {
                context.getCounter(Counters.ERROR).increment(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static Job createSubmittableJob(final Configuration conf, final String[] args) throws IOException {

        final Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(10000);

        final String tableName = args[1];
        final long startTime = Long.parseLong(args[2]);
        final long endTime = Long.parseLong(args[3]);
        final String outputFilePath = args[4];

        scan.setTimeRange(startTime, endTime);

        Job job = new Job(conf, NAME + "_" + tableName);
        job.setJarByClass(ComputeRowHash.class);

        TableMapReduceUtil.initTableMapperJob(tableName, scan,
                ComputeHash.class, ImmutableBytesWritable.class, Text.class, job);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath));

        return job;
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 5) {
            System.err.println("Usage: " + NAME + " <table-name> <start-time> <end-time> <output-file>");
            System.exit(1);
        }

        final Configuration conf = HBaseConfiguration.create();
        final Job job = createSubmittableJob(conf, args);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
