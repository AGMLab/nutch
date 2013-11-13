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

import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TrustRankJob {

  public static final Logger LOG = LoggerFactory.getLogger(TrustRankJob.class);
  public TrustRankJob(String[] args) {
    try {
      run(args);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public Map<String, Object> run(String[] args) throws Exception {
    LOG.info("TrustRank starts...");
    TrustRankGiraphJob TrustRankJob = new TrustRankGiraphJob();
    TrustRankJob.run(args);
    LOG.info("TrustRank has finished...");
    return null;
  }

//  private int updateTable() throws Exception {
//    LOG.info("ScoreUpdaterJob: starting");
//    run(ToolUtil.toArgMap(Nutch.ARG_CRAWL, 0));
//    LOG.info("ScoreUpdaterJob: done");
//    return 0;
//  }
//
//  public int run(String[] args) throws Exception {
//    return updateTable();
//  }

  public static void main(String[] args) throws Exception {
    //int res = ToolRunner.run(NutchConfiguration.create(),
    // new ScoreUpdaterJob(), args);
    new TrustRankJob(args);

  }

}
