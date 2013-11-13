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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HostRankJob {

  public static final Logger LOG = LoggerFactory.getLogger(HostRankJob.class);
  public HostRankJob(String[] args) {
    try {
      run(args);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public Map<String, Object> run(String[] args) throws Exception {
    LOG.info("HostRank starts...");
    HostRankGiraphJob HostRankJob = new HostRankGiraphJob();    
    HostRankJob.run(args);
    LOG.info("HostRank has finished...");
    return null;
  }

  public static void main(String[] args) throws Exception {
    //int res = ToolRunner.run(NutchConfiguration.create(),
    // new ScoreUpdaterJob(), args);
    new HostRankJob(args);

  }

}
