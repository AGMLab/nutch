package org.apache.nutch.scoring;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.examples.LinkRank.LinkRankComputation;
import org.apache.giraph.examples.LinkRank.LinkRankVertexMasterCompute;
import org.apache.giraph.examples.LinkRank.NutchTableEdgeInputFormat;
import org.apache.giraph.examples.LinkRank.NutchTableEdgeOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;


public class LinkRankGiraphJob implements Tool {
  private static final Logger LOG = Logger.getLogger(LinkRankGiraphJob.class);
  private GiraphConfiguration conf;
  private String INPUT_TABLE_NAME = "webpage";
  private String OUTPUT_TABLE_NAME = "webpage";

  @Override
  public int run(String[] strings) throws Exception {

    LOG.info("Starting LinkRank Giraph Job");

    Configuration config = HBaseConfiguration.create();
    /*
    // below are required if Nutch runs in local mode.
    // but not required when run via deploy/bin/nutch.
    config.clear();
    config.set("hbase.zookeeper.quorum", "localhost");
    config.set("hbase.zookeeper.property.clientPort", "2181");
    config.set("hbase.master", "localhost:60000");
    config.set("mapred.job.tracker", "localhost:9001");
    */

    HBaseAdmin admin = new HBaseAdmin(config);
    ZooKeeperWatcher zooKeeperWatcher = new ZooKeeperWatcher(config, "zkw", new Abortable() {
      @Override
      public void abort(String s, Throwable throwable) {
        System.out.println(s);
      }
    });

    admin.getMaster();

    GiraphConfiguration giraphConf = new GiraphConfiguration(config);
    giraphConf.setZooKeeperConfiguration(
            zooKeeperWatcher.getQuorum());
    giraphConf.setComputationClass(LinkRankComputation.class);
    giraphConf.setMasterComputeClass(LinkRankVertexMasterCompute.class);
    giraphConf.setOutEdgesClass(ByteArrayEdges.class);
    giraphConf.setVertexInputFormatClass(NutchTableEdgeInputFormat.class);
    giraphConf.setVertexOutputFormatClass(NutchTableEdgeOutputFormat.class);
    giraphConf.setInt("giraph.linkRank.superstepCount", 10);
    giraphConf.setWorkerConfiguration(1, 1, 100.0f);
    giraphConf.set(TableInputFormat.INPUT_TABLE, INPUT_TABLE_NAME);
    giraphConf.set(TableOutputFormat.OUTPUT_TABLE, OUTPUT_TABLE_NAME);

    GiraphJob giraphJob = new GiraphJob(giraphConf, "LinkRank");
    return giraphJob.run(true) ? 0: -1;

  }

  @Override
  public void setConf(final Configuration conf) {
    this.conf = new GiraphConfiguration(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
