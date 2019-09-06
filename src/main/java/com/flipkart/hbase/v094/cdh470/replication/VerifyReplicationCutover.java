package com.flipkart.hbase.v094.cdh470.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * This map-only job compares the data from a local table with a remote one.
 * Every cell is compared and must have exactly the same keys (even timestamp)
 * as well as same value. It is possible to restrict the job by time range and
 * families. The peer id that's provided must match the one given when the
 * replication stream was setup.
 * <p>
 * Three counters are provided, Verifier.Counters.GOODROWS, BADROWS and UPDATEDROWS. The reason
 * for a why a row is different is shown in the map's log.
 */
public class VerifyReplicationCutover {

    private static final Log LOG =
            LogFactory.getLog(VerifyReplicationCutover.class);

    private static final String NAME = "verifyrep";
    static long startTime = 0;
    static long endTime = 0;
    static String tableName = null;
    static String families = null;
    static String peerId = null;

    /**
     * Map-only comparator for 2 tables
     */
    public static class Verifier
            extends TableMapper<ImmutableBytesWritable, Put> {

        public enum Counters {GOODROWS, BADROWS, UPDATEDROWS}

        private HTable replicatedTable;
        /**
         * Map method that compares every scanned row with the equivalent from
         * a distant cluster.
         * @param row  The current table row key.
         * @param value  The columns.
         * @param context  The current context.
         * @throws IOException When something is broken with the data.
         */
        @Override
        public void map(ImmutableBytesWritable row, final Result value,
                        Context context)
                throws IOException {
            if (replicatedTable == null) {
                Configuration conf = context.getConfiguration();

                HConnectionManager.execute(new HConnectionManager.HConnectable<Void>(conf) {
                    @Override
                    public Void connect(HConnection conn) throws IOException {
                        try {
                            ReplicationZookeeper zk = new ReplicationZookeeper(conn, conf,
                                    conn.getZooKeeperWatcher());
                            ReplicationPeer peer = zk.getPeer(conf.get(NAME+".peerId"));
                            replicatedTable = new HTable(peer.getConfiguration(),
                                    conf.get(NAME+".tableName"));
                        } catch (KeeperException e) {
                            throw new IOException("Got a ZK exception", e);
                        }
                        return null;
                    }
                });
            }

            Result res = replicatedTable.get(new Get(value.getRow()));
            try {
                Result.compareResults(value, res);
                context.getCounter(Counters.GOODROWS).increment(1);
            } catch (Exception e) {
                // Check if it's really a bad row or simply updated
                // It's better to check here as even different length KVs can
                // throw exceptions
                if (isBad(value, res)) {
                    context.getCounter(Counters.BADROWS).increment(1);
                    LOG.warn("Bad row : ("+ Bytes.toString(value.getRow())+ ": "+Bytes.toString(res.getRow())+")", e);
                } else {
                    LOG.info("Row updated: " + new String(value.getRow()));
                    context.getCounter(Counters.UPDATEDROWS).increment(1);
                }
            }
        }

        /**
         *
         * @param old
         * @param res
         * @return true even if a single key-value pair is newer than
         * the older one
         */
        private boolean isBad(final Result old, final Result res) {
            if(!Bytes.equals(old.getRow(),res.getRow())){

                LOG.error("Row ID "+Bytes.toString(old.getRow()) + " not found in destination , please check , insted found : "+Bytes.toString(res.getRow()));
                return false;
            }
            final KeyValue[] oldKVs = old.raw();
            final KeyValue[] newKVs = res.raw();
            boolean isBad = false, possiblyDeleted = false;


            if (old.size() > res.size()) {
                possiblyDeleted = true;
            }

            try {
                for (int i = 0, n = old.size(), N = res.size(); i < n && i < N && !isBad; ++i) {
                    if (oldKVs[i].matchingFamily(newKVs[i])
                            && oldKVs[i].matchingQualifier(newKVs[i])) {
                        isBad = oldKVs[i].getTimestamp() > newKVs[i].getTimestamp();
                    }
                }

                if (!isBad) {
                    LOG.info("Old: " + old.toString());
                    LOG.info("New: " + res.toString());
                }
            } catch (Exception e) {
                LOG.error("An error occurred in isBad", e);
            }

            return !(possiblyDeleted && !isBad) && isBad;
        }

        protected void cleanup(Context context) {
            if (replicatedTable != null) {
                try {
                    replicatedTable.close();
                    replicatedTable = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static String getPeerQuorumAddress(final Configuration conf) throws IOException {
        HConnection conn = HConnectionManager.getConnection(conf);
        try {
            ReplicationZookeeper zk = new ReplicationZookeeper(conn, conf,
                    conn.getZooKeeperWatcher());

            ReplicationPeer peer = zk.getPeer(peerId);
            if (peer == null) {
                throw new IOException("Couldn't get peer conf!");
            }

            Configuration peerConf = peer.getConfiguration();
            return ZKUtil.getZooKeeperClusterKey(peerConf);
        } catch (KeeperException e) {
            throw new IOException("Got a ZK exception", e);
        } finally {
            conn.close();
        }
    }

    /**
     * Sets up the actual job.
     *
     * @param conf  The current configuration.
     * @param args  The command line parameters.
     * @return The newly created job.
     * @throws java.io.IOException When setting up the job fails.
     */
    public static Job createSubmittableJob(Configuration conf, String[] args)
            throws IOException {
        if (!doCommandLine(args)) {
            return null;
        }
        if (!conf.getBoolean(HConstants.REPLICATION_ENABLE_KEY, false)) {
            throw new IOException("Replication needs to be enabled to verify it.");
        }
        HConnectionManager.execute(new HConnectionManager.HConnectable<Void>(conf) {
            @Override
            public Void connect(HConnection conn) throws IOException {
                try {
                    ReplicationZookeeper zk = new ReplicationZookeeper(conn, conf,
                            conn.getZooKeeperWatcher());
                    // Just verifying it we can connect
                    ReplicationPeer peer = zk.getPeer(peerId);
                    if (peer == null) {
                        throw new IOException("Couldn't get access to the slave cluster," +
                                "please see the log");
                    }
                } catch (KeeperException ex) {
                    throw new IOException("Couldn't get access to the slave cluster" +
                            " because: ", ex);
                }
                return null;
            }
        });
        conf.set(NAME+".peerId", peerId);
        conf.set(NAME+".tableName", tableName);
        conf.setLong(NAME+".startTime", startTime);
        conf.setLong(NAME+".endTime", endTime);
        if (families != null) {
            conf.set(NAME+".families", families);
        }

        String peerQuorumAddress = getPeerQuorumAddress(conf);
        conf.set(NAME + ".peerQuorumAddress", peerQuorumAddress);
        LOG.info("Peer Quorum Address: " + peerQuorumAddress);

        Job job = new Job(conf, NAME + "_" + tableName);
        job.setJarByClass(VerifyReplicationCutover.class);

        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(10000);

        if (startTime != 0) {
            scan.setTimeRange(startTime,
                    endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
        }
        if(families != null) {
            String[] fams = families.split(",");
            for(String fam : fams) {
                scan.addFamily(Bytes.toBytes(fam));
            }
        }
        TableMapReduceUtil.initTableMapperJob(tableName, scan,
                Verifier.class, null, null, job);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);
        return job;
    }

    private static boolean doCommandLine(final String[] args) {
        if (args.length < 2) {
            printUsage(null);
            return false;
        }
        try {
            for (int i = 0; i < args.length; i++) {
                String cmd = args[i];
                if (cmd.equals("-h") || cmd.startsWith("--h")) {
                    printUsage(null);
                    return false;
                }

                final String startTimeArgKey = "--starttime=";
                if (cmd.startsWith(startTimeArgKey)) {
                    startTime = Long.parseLong(cmd.substring(startTimeArgKey.length()));
                    continue;
                }

                final String endTimeArgKey = "--endtime=";
                if (cmd.startsWith(endTimeArgKey)) {
                    endTime = Long.parseLong(cmd.substring(endTimeArgKey.length()));
                    continue;
                }

                final String familiesArgKey = "--families=";
                if (cmd.startsWith(familiesArgKey)) {
                    families = cmd.substring(familiesArgKey.length());
                    continue;
                }

                if (i == args.length-2) {
                    peerId = cmd;
                }

                if (i == args.length-1) {
                    tableName = cmd;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            printUsage("Can't start because " + e.getMessage());
            return false;
        }
        return true;
    }

    /*
     * @param errorMsg Error message.  Can be null.
     */
    private static void printUsage(final String errorMsg) {
        if (errorMsg != null && errorMsg.length() > 0) {
            System.err.println("ERROR: " + errorMsg);
        }
        System.err.println("Usage: verifyrep [--starttime=X]" +
                " [--stoptime=Y] [--families=A] <peerid> <tablename>");
        System.err.println();
        System.err.println("Options:");
        System.err.println(" starttime    beginning of the time range");
        System.err.println("              without endtime means from starttime to forever");
        System.err.println(" endtime     end of the time range");
        System.err.println(" families     comma-separated list of families to copy");
        System.err.println();
        System.err.println("Args:");
        System.err.println(" peerid       Id of the peer used for verification, must match the one given for replication");
        System.err.println(" tablename    Name of the table to verify");
        System.err.println();
        System.err.println("Examples:");
        System.err.println(" To verify the data replicated from TestTable for a 1 hour window with peer #5 ");
        System.err.println(" $ hbase " +
                "com.flipkart.hbase.v094.cdh470.replication.VerifyReplicationCutover" +
                " --starttime=1265875194289 --stoptime=1265878794289 5 TestTable ");
    }

    /**
     * Main entry point.
     *
     * @param args  The command line parameters.
     * @throws Exception When running the job fails.
     */
    public static void main(String[] args) throws Exception {
        LOG.info("Hello there, this is in prep for hoodoo cutover.");
        System.out.println("A load of bull");
        Configuration conf = HBaseConfiguration.create();
        Job job = createSubmittableJob(conf, args);
        if (job != null) {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}

