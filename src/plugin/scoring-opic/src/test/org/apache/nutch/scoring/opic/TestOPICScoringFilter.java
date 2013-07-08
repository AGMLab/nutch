/**
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
 */
package org.apache.nutch.scoring.opic;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;

import java.text.DecimalFormat;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * JUnit test for <code>OPICScoringFilter</code>.
 *
 * @author Talat Uyarer
 * @author Yasin Kilinc
 */
public class TestOPICScoringFilter extends TestCase {

	private Map<String, String[]> linkList = new LinkedHashMap<String,String[]>();
	private final List<ScoreDatum> outlinkedScoreData = new ArrayList<ScoreDatum>();
	private List<ScoreDatum> inlinkedScoreData = new ArrayList<ScoreDatum>();
	
	private static final int DEPTH = 3;
	
	DecimalFormat df = new DecimalFormat("#.###");
	
	
	private final String[] seedList = new String[] {
	    "http://a.com",
	    "http://b.com",
	    "http://c.com",
	  };
	
	private void fillLinks() {
		linkList.put("http://a.com", new String[]{"http://b.com"});
		linkList.put("http://b.com", new String[]{"http://a.com", "http://c.com"});
		linkList.put("http://c.com", new String[]{"http://a.com", "http://b.com","http://d.com"});
		linkList.put("http://d.com", new String[]{});
	}
	
	  
	  private static HashMap<Integer,HashMap<String,Float>> acceptedScores = new HashMap<Integer,HashMap<String,Float>>(){{
		  put(1,new HashMap<String, Float>(){{
			  put(new String("http://a.com"),new Float(1.833));
			  put(new String("http://b.com"),new Float(2.333));
			  put(new String("http://c.com"),new Float(1.5));
			  put(new String("http://d.com"),new Float(0.333));
		  }});
		  put(2,new HashMap<String, Float>(){{
			  put(new String("http://a.com"),new Float(2.666));
			  put(new String("http://b.com"),new Float(3.333));
			  put(new String("http://c.com"),new Float(2.166));
			  put(new String("http://d.com"),new Float(0.278));
		  }});
		  put(3,new HashMap<String, Float>(){{
			  put(new String("http://a.com"),new Float(3.388));
			  put(new String("http://b.com"),new Float(4.388));
			  put(new String("http://c.com"),new Float(2.666));
			  put(new String("http://d.com"),new Float(0.5));
		  }});
	  }};
	  
	  private HashMap<Integer,HashMap<String,Float>> resultScores = new HashMap<Integer,HashMap<String,Float>>();
	  
	  private OPICScoringFilter scoringFilter = null;
	  
	  public static Test suite() {
	    return new TestSuite(TestOPICScoringFilter.class);
	  }
	  
	  public static void main(String[] args) {
	    TestRunner.run(suite());
	  }
	  
	  public void setUp() throws Exception {
		  
		// Storing values on Map instead of DB
		Map<String,Map<WebPage,List<ScoreDatum>>> dbWebPages = new LinkedHashMap<String,Map<WebPage,List<ScoreDatum>>>();
		
		// Storing Row status. If row is new value must be True
		Map<String,Boolean> dbWebPagesControl = new LinkedHashMap<String,Boolean>();
		
		TestOPICScoringFilter self = new TestOPICScoringFilter();
		self.fillLinks();
		
		Configuration conf  = NutchConfiguration.create();
		float  scoreInjected = conf.getFloat("db.score.injected",1.0f);
		
		scoringFilter = new OPICScoringFilter();
		scoringFilter.setConf(conf);
		
	    // inject seed list
		for (String url : self.seedList) {
			WebPage row = new WebPage();
			row.setScore(scoreInjected);
			scoringFilter.injectedScore(url, row);
			
			List<ScoreDatum> scList = new LinkedList<ScoreDatum>();
			Map<WebPage,List<ScoreDatum>> webPageMap = new HashMap<WebPage,List<ScoreDatum>>();
			webPageMap.put(row, scList);
			dbWebPages.put(TableUtil.reverseUrl(url),webPageMap);
			dbWebPagesControl.put(TableUtil.reverseUrl(url), true);
		}
		
		// Depth Loop
		for (int i = 1; i <= DEPTH; i++) {
	        Iterator<Map.Entry<String,Map<WebPage,List<ScoreDatum>>>> iter = dbWebPages.entrySet().iterator();
	        
	        // Simulate Mapping
	        while (iter.hasNext()) {
	        	Map.Entry<String,Map<WebPage,List<ScoreDatum>>> entry = iter.next();
	        	Map<WebPage,List<ScoreDatum>> webPageMap = entry.getValue();
	        	
	        	WebPage row = null;
	        	List<ScoreDatum> scoreList = null;
	        	Iterator<Map.Entry<WebPage,List<ScoreDatum>>> iters = webPageMap.entrySet().iterator();
	            if(iters.hasNext()) {
	            	Map.Entry<WebPage,List<ScoreDatum>> values = iters.next();
	            	row = values.getKey();
	            	scoreList = values.getValue();
	            }
	        	
				String reverseUrl = entry.getKey();
				String url = TableUtil.unreverseUrl(reverseUrl);
				float score = row.getScore();
				
				if(dbWebPagesControl.get(TableUtil.reverseUrl(url))){
					row.setScore(scoringFilter.generatorSortValue(url, row, score));
					dbWebPagesControl.put(TableUtil.reverseUrl(url), false);
				}
				
				// adding outlinks from testdata
				String[] seedOutlinks = self.linkList.get(url);
				for (String seedOutlink : seedOutlinks){
						row.putToOutlinks(new Utf8(seedOutlink), new Utf8());
				}
				
				self.outlinkedScoreData.clear();
			    Map<Utf8, Utf8> outlinks = row.getOutlinks();
			    if (outlinks != null) {
			      for (Entry<Utf8, Utf8> e : outlinks.entrySet()) {
			        int depth=Integer.MAX_VALUE;
			        self.outlinkedScoreData.add(new ScoreDatum(0.0f, e.getKey().toString(), e.getValue().toString(), depth));
			      }
			    }
			    scoringFilter.distributeScoreToOutlinks(url, row, self.outlinkedScoreData, (outlinks == null ? 0 : outlinks.size()));
			    
			    for (ScoreDatum sc : self.outlinkedScoreData) {
				    if(dbWebPages.get(TableUtil.reverseUrl(sc.getUrl()))==null){
						WebPage outlinkRow = new WebPage();
						scoringFilter.initialScore(sc.getUrl(), outlinkRow);
						List<ScoreDatum> newScoreList = new LinkedList<ScoreDatum>();
						newScoreList.add(sc);
						Map<WebPage,List<ScoreDatum>> values = new HashMap<WebPage,List<ScoreDatum>>();
						values.put(outlinkRow, newScoreList);
						dbWebPages.put(TableUtil.reverseUrl(sc.getUrl()),values);
						dbWebPagesControl.put(TableUtil.reverseUrl(sc.getUrl()), true);
					}else{
						Map<WebPage,List<ScoreDatum>> values = dbWebPages.get(TableUtil.reverseUrl(sc.getUrl()));
						Iterator<Map.Entry<WebPage,List<ScoreDatum>>> value = values.entrySet().iterator();
						if(value.hasNext()){
							Map.Entry<WebPage,List<ScoreDatum>> list = value.next();
			            	scoreList = list.getValue();
							scoreList.add(sc);
						}
					}
			     }
			}
	        
	        // Simulate Reducing
	        for (Map.Entry<String, Map<WebPage, List<ScoreDatum>>> page : dbWebPages.entrySet()) {
	        	
	        	String reversedUrl = page.getKey();
	        	String url = TableUtil.unreverseUrl(reversedUrl);
	        	
	        	Iterator<Map.Entry<WebPage, List<ScoreDatum>>> rr = page.getValue().entrySet().iterator();
	        	
	        	List<ScoreDatum> inlinkedScoreDataList = null;
	        	WebPage row = null;
	        	if(rr.hasNext()){
	        		Map.Entry<WebPage,List<ScoreDatum>> aa = rr.next();
	        		inlinkedScoreDataList = aa.getValue();
	        		row = aa.getKey();
	        	}
	        	scoringFilter.updateScore(url, row, inlinkedScoreDataList);
	        	inlinkedScoreDataList.clear();
	  		    HashMap<String, Float> result = new HashMap<String, Float>();
	  		    result.put(url,row.getScore());
	        	
	        	resultScores.put(i,result);
	        }
        
		}
	  }
	  
	  public void testModeAccept() {
	    for (int i = 1; i <= DEPTH; i++) {
	    	for (String resultUrl : resultScores.get(i).keySet()) {
	    		String accepted = df.format(acceptedScores.get(i).get(resultUrl));	    		
	    		String result = df.format(resultScores.get(i).get(resultUrl));
	    		assertTrue(accepted.equals(result));
			}
	    }
	    
	  }	
	
}
