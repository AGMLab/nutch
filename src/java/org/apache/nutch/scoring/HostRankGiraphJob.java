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
import org.apache.nutch.api.ConfResource;
import org.apache.nutch.api.NutchApp;


public class HostRankGiraphJob extends LinkRankGiraphJob {
  private static final Logger LOG = Logger.getLogger(HostRankGiraphJob.class);
  private GiraphConfiguration conf;
  private String INPUT_TABLE_NAME = "host";
  private String OUTPUT_TABLE_NAME = "webpage";
  private String QUALIFIER = "hostrank";


}
