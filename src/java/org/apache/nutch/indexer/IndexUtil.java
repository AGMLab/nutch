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
package org.apache.nutch.indexer;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.host.HostDb;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Host;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;

import javax.sound.midi.Soundbank;

/**
 * Utility to create an indexed document from a webpage.  
 *
 */
public class IndexUtil {
  private static final Log LOG = LogFactory.getLog(new Object() {
  }.getClass().getEnclosingClass());
  
  
  private IndexingFilters filters;
  private ScoringFilters scoringFilters;
  private Configuration conf;
  private HostDb hostDb;
  public IndexUtil(Configuration conf) {
    this.conf = NutchConfiguration.create();
    try {
		this.hostDb = new HostDb(conf);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    LOG.info(this.conf.get("fetcher.queue.use.host.settings"));

    filters = new IndexingFilters(conf);
    scoringFilters = new ScoringFilters(conf);
  }
  
  /**
   * Index a webpage.
   * 
   * @param key The key of the page (reversed url).
   * @param page The webpage.
   * @return The indexed document, or null if skipped by index filters.
   */
  public NutchDocument index(String key, WebPage page) {
    NutchDocument doc = new NutchDocument();
    doc.add("id", key);
    doc.add("digest", StringUtil.toHexString(page.getSignature().array()));

    String url = TableUtil.unreverseUrl(key);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Indexing URL: " + url);
    }

    try {
      doc = filters.filter(doc, url, page);
    } catch (IndexingException e) {
      LOG.warn("Error indexing "+key+": "+e);
      return null;
    }

    // skip documents discarded by indexing filters
    if (doc == null) return null;

    float boost = 1.0f;
    // run scoring filters
    try {
      boost = scoringFilters.indexerScore(url, doc, page, boost);
    } catch (final ScoringFilterException e) {
      LOG.warn("Error calculating score " + key + ": " + e);
      return null;
    }

    doc.setScore(boost);
    // store boost for use by explain and dedup
    doc.add("boost", Float.toString(boost));

    
    String reprUrl = null;
    if (page.isReadable(WebPage.Field.REPR_URL.getIndex())) {
      reprUrl = TableUtil.toString(page.getReprUrl());
    }
    
    // Index HostRank score.
    double hostRankScore = getHostRankScore(url, reprUrl);
    doc.add("hostrank", Double.toString(hostRankScore));
    
    // Index LinkRank score.
    double linkRankScore = page.getFromMetadata(new Utf8("_lr_")).getDouble();
    doc.add("linkrank", Double.toString(linkRankScore));

    return doc;
  }
  
  /**
   * Returns the HostRank score for the given url.
   * @param reprUrl
   * @param url
   * @return
   */
  private double getHostRankScore(String url, String reprUrl){
    double hostrankScore = 0.0;
    String host = null;
    try {
      URL u;
      if (reprUrl != null) {
        u = new URL(reprUrl);
      } else {
        u = new URL(url);
      }

      host = u.getHost();
      System.out.println("Host: " + host);
      Host h = hostDb.getByHostName(host);    
      System.out.println("Metadata: =====" + h.getMetadata().keySet());

      try {
        ByteBuffer b = h.getFromMetadata(new Utf8("_hr_"));
        hostrankScore = Bytes.toDouble(b.array());
      } catch (NullPointerException e){
        //e.printStackTrace();
        LOG.info("No _hr_ for " + h);
      }
      
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return hostrankScore;

  }
  
}
