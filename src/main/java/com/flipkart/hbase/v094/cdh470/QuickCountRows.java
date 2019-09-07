package com.flipkart.hbase.v094.cdh470;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

public class QuickCountRows {

    private static final String NAME = "quickcount";

    public enum Counters {ROWS}

    public static class Counter
            extends TableMapper<ImmutableBytesWritable, Put> {

        @Override
        public void map(ImmutableBytesWritable row, final Result value, Context context) {

            context.getCounter(Counters.ROWS).increment(1);
        }
    }

    public static Job createSubmittableJob(final Configuration conf, final String[] args) throws IOException {

        final Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(10000);

        final String tableName = args[args.length - 1];

        Job job = new Job(conf, NAME + "_" + tableName);
        job.setJarByClass(QuickCountRows.class);

        TableMapReduceUtil.initTableMapperJob(args[args.length - 1], scan,
                QuickCountRows.Counter.class, null, null, job);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);

        return job;
    }

    public static void main(String[] args) throws Exception {

        final Configuration conf = HBaseConfiguration.create();
        final Job job = createSubmittableJob(conf, args);

        if (job != null) {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
