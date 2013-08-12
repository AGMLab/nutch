package org.apache.nutch.scoring;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.nutch.LinkRank.LinkRankComputation;
import org.apache.giraph.nutch.LinkRank.LinkRankVertexMasterCompute;
import org.apache.giraph.nutch.LinkRank.LinkRankVertexWorkerContext;
import org.apache.giraph.nutch.LinkRank.Nutch2WebpageInputFormat;
import org.apache.giraph.nutch.LinkRank.Nutch2WebpageOutputFormat;
import org.apache.giraph.nutch.LinkRank.LinkRankVertexFilter;
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
import org.apache.nutch.api.ConfResource;
import org.apache.nutch.api.DbReader;
import org.apache.nutch.api.NutchApp;


public class LinkRankGiraphJob implements Tool {
  private Logger LOG = Logger.getLogger(getClass());
  private Configuration conf;
  private String INPUT_TABLE_NAME = "webpage";
  private String OUTPUT_TABLE_NAME = "webpage";
  private String QUALIFIER = "_lr_";

  public void setup() {
    String confId = ConfResource.DEFAULT_CONF;
    Configuration nutchConf = NutchApp.confMgr.get(confId);
    if (getClass() == LinkRankGiraphJob.class){
      INPUT_TABLE_NAME = nutchConf.get("storage.schema.webpage");
      OUTPUT_TABLE_NAME = nutchConf.get("storage.schema.webpage");
      QUALIFIER = "_lr_";
    } else {
      INPUT_TABLE_NAME = nutchConf.get("storage.schema.host");
      OUTPUT_TABLE_NAME = "hostrank";
      QUALIFIER = "_hr_";
    }
    LOG.info("Using HBase table of Nutch: " + INPUT_TABLE_NAME);
  }

  @Override
  public int run(String[] strings) throws Exception {

    LOG.info("Starting " + getClass().getName());
    setup();
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
    giraphConf.setWorkerContextClass(LinkRankVertexWorkerContext.class);
    giraphConf.setVertexInputFormatClass(Nutch2WebpageInputFormat.class);
    giraphConf.setVertexOutputFormatClass(Nutch2WebpageOutputFormat.class);
    giraphConf.setInt("giraph.linkRank.superstepCount", 10);
    giraphConf.setInt("giraph.linkRank.scale", 10);
    giraphConf.set("giraph.linkRank.family", "mtdt");
    giraphConf.set("giraph.linkRank.qualifier", QUALIFIER);
    giraphConf.setVertexInputFilterClass(LinkRankVertexFilter.class);
    giraphConf.setWorkerConfiguration(1, 1, 100.0f);
    LOG.info("setting input table as " + INPUT_TABLE_NAME);
    giraphConf.set(TableInputFormat.INPUT_TABLE, INPUT_TABLE_NAME);
    giraphConf.set(TableOutputFormat.OUTPUT_TABLE, OUTPUT_TABLE_NAME);



    LOG.info("Table input: ========= " + giraphConf.get(TableInputFormat.INPUT_TABLE));

    GiraphJob giraphJob = new GiraphJob(giraphConf, getClass().getName());
    return giraphJob.run(true) ? 0: -1;

  }

  @Override
  public void setConf(final Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
