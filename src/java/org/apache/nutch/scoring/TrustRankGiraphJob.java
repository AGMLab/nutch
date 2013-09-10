package org.apache.nutch.scoring;

import org.apache.giraph.ranking.LinkRank.TrustRankComputation;
import org.apache.giraph.ranking.LinkRank.TrustRankVertexMasterCompute;
import org.apache.giraph.ranking.LinkRank.io.HostRankVertexFilter;
import org.apache.giraph.ranking.LinkRank.io.Nutch2HostInputFormat;
import org.apache.giraph.ranking.LinkRank.io.Nutch2HostOutputFormat;
import org.apache.giraph.ranking.LinkRank.io.Nutch2HostTrustInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.api.ConfResource;
import org.apache.nutch.api.NutchApp;

public class TrustRankGiraphJob extends AbstractGiraphJob {
	public void setup() {
	    String confId = ConfResource.DEFAULT_CONF;
	    Configuration nutchConf = NutchApp.confMgr.get(confId);
	    LOG.info("=========HostRank=========");
	    INPUT_FORMAT = Nutch2HostTrustInputFormat.class;
	    OUTPUT_FORMAT = Nutch2HostOutputFormat.class;
	    VERTEX_FILTER = HostRankVertexFilter.class;
	    INPUT_TABLE_NAME = nutchConf.get("storage.schema.host", "host");
	    OUTPUT_TABLE_NAME = "host";
	    QUALIFIER = "_tr_";
	    COMPUTATION = TrustRankComputation.class;
	    MASTER_COMPUTE = TrustRankVertexMasterCompute.class;
	}
}
