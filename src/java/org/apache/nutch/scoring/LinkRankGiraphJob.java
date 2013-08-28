package org.apache.nutch.scoring;

import org.apache.giraph.nutch.LinkRank.io.formats.Nutch2WebpageInputFormat;
import org.apache.giraph.nutch.LinkRank.io.formats.Nutch2WebpageOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.api.ConfResource;
import org.apache.nutch.api.NutchApp;


public class LinkRankGiraphJob extends AbstractGiraphJob {
  public void setup() {
    String confId = ConfResource.DEFAULT_CONF;
    Configuration nutchConf = NutchApp.confMgr.get(confId);
    LOG.info("=========LinkRank=========");
    INPUT_FORMAT = Nutch2WebpageInputFormat.class;
    OUTPUT_FORMAT = Nutch2WebpageOutputFormat.class;
    INPUT_TABLE_NAME = nutchConf.get("storage.schema.webpage", "webpage");
    OUTPUT_TABLE_NAME = nutchConf.get("storage.schema.webpage", "webpage");
    QUALIFIER = "_lr_";
    LOG.info("Using HBase table of Nutch: " + INPUT_TABLE_NAME);
  }
}
