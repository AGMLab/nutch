package org.apache.nutch.scoring;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.ranking.LinkRank.LinkRankComputation;
import org.apache.giraph.ranking.LinkRank.LinkRankVertexMasterCompute;
import org.apache.giraph.ranking.LinkRank.io.LinkRankVertexFilter;
import org.apache.giraph.ranking.LinkRank.io.Nutch2WebpageInputFormat;
import org.apache.giraph.ranking.LinkRank.io.Nutch2WebpageOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;


public abstract class AbstractGiraphJob implements Tool {
  protected Logger LOG = Logger.getLogger(getClass());
  protected Configuration conf;
  protected String INPUT_TABLE_NAME = "webpage";
  protected String OUTPUT_TABLE_NAME = "webpage";
  protected String QUALIFIER = "_lr_";
  protected Class INPUT_FORMAT = Nutch2WebpageInputFormat.class;
  protected Class VERTEX_FILTER = LinkRankVertexFilter.class;
  protected Class OUTPUT_FORMAT = Nutch2WebpageOutputFormat.class;
  protected Class COMPUTATION = LinkRankComputation.class;
  protected Class MASTER_COMPUTE = LinkRankVertexMasterCompute.class;

  /**
   * Subclasses should implement this. 
   * Inside should be format classes, table names, qualifier strings.
   */
  public abstract void setup();

  /**
   * Run method for the giraph job. No need for overriding this.
   */
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
    giraphConf.setComputationClass(COMPUTATION);
    giraphConf.setMasterComputeClass(MASTER_COMPUTE);
    giraphConf.setOutEdgesClass(ByteArrayEdges.class);
    giraphConf.setVertexInputFormatClass(INPUT_FORMAT);
    giraphConf.setVertexOutputFormatClass(OUTPUT_FORMAT);
    //giraphConf.setWorkerConfiguration(minWorkers, maxWorkers, minPercentResponded)
    giraphConf.setInt("giraph.linkRank.superstepCount", 20);
    giraphConf.setInt("giraph.linkRank.scale", 10);
    giraphConf.set("giraph.linkRank.family", "mtdt");
    giraphConf.set("giraph.linkRank.qualifier", QUALIFIER);
    giraphConf.setVertexInputFilterClass(VERTEX_FILTER);
    giraphConf.setWorkerConfiguration(1, 1, 100.0f);
    LOG.info("setting input table as " + INPUT_TABLE_NAME);
    LOG.info("setting output table as " + OUTPUT_TABLE_NAME);
    giraphConf.set(TableInputFormat.INPUT_TABLE, INPUT_TABLE_NAME);
    giraphConf.set(TableOutputFormat.OUTPUT_TABLE, OUTPUT_TABLE_NAME);
 
    
    LOG.info("Table input: ========= " + giraphConf.get(TableInputFormat.INPUT_TABLE));
    GiraphJob giraphJob = new GiraphJob(giraphConf, getClass().getName());
    /**
     * If using HDFS for output use lines below:,
     * FileOutputFormat.setOutputPath(new JobConf(giraphJob.getConfiguration()), new Path("/user/emre/hostrank"));
     * FileOutputFormat.setOutputPath(giraphJob.getInternalJob(), new Path("/user/emre/hostrank"));  
    */
    admin.close();
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
