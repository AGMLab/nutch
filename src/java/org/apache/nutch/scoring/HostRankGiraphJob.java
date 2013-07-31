package org.apache.nutch.scoring;

import org.apache.log4j.Logger;

public class HostRankGiraphJob extends LinkRankGiraphJob {
  private static final Logger LOG = Logger.getLogger(HostRankGiraphJob.class);
  private String INPUT_TABLE_NAME = "host";
  private String OUTPUT_TABLE_NAME = "webpage";
  private String QUALIFIER = "hostrank";
}
