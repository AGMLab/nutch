/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.nutch.scoring;

import org.apache.avro.util.Utf8;
import org.apache.giraph.io.formats.PseudoRandomIntNullVertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LinkRankJob {

  public static final Logger LOG = LoggerFactory.getLogger(LinkRankJob.class);
  public LinkRankJob(String[] args) {
    try {
      run(args);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public Map<String, Object> run(String[] args) throws Exception {
    LOG.info("LinkRank starts...");
    LinkRankGiraphJob linkRankJob = new LinkRankGiraphJob();
//    linkRankJob.run(null);
    linkRankJob.run(args);
    LOG.info("LinkRank has finished...");
    return null;
  }

//  public int run(String[] args) throws Exception {
//    LOG.info("ScoreUpdaterJob: starting");
//    run(ToolUtil.toArgMap((Object[])args));
//    LOG.info("ScoreUpdaterJob: done");
//    return 0;
//  }

  public static void main(String[] args) throws Exception {
    //int res = ToolRunner.run(NutchConfiguration.create(),
    // new ScoreUpdaterJob(), args);
    new LinkRankJob(args);

  }

}
