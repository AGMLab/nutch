package org.apache.nutch.comparison;
 
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
 
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ReadFromHBase {
	public static double[] arrr;
	private static String TABLE_NAME = "host";
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
    	start(args);
    }
    
    public static void start(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
    	for (int i = 0; i < args.length; i++){
	    	if(args[i].equals("-p")){
	    		TABLE_NAME=args[i+1]+"_"+TABLE_NAME;
	    	}
    	}
	    @SuppressWarnings("deprecation")
		Configuration config = new HBaseConfiguration();
//	    config.addResource("/usr/lib/hbase/conf/hbase-site.xml");
//	    config.set("hbase.zookeeper.property.clientPort", "9983");
//	    config.set("hbase.zookeeper.quorum", "host-10-6-149-119");
	    
	    //config.set("dfs.block.size", "33554432");
	    Job job = new Job(config, "HBase Read From host");
	    job.setJarByClass(ReadFromHBase.class);     // class that contains mapper
	    
	    //job.setSortComparatorClass(KeyComparator.class);
	    System.out.println("creating scan obj");
	    Scan scan = new Scan();
	    scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
	    scan.setCacheBlocks(false);  // don't set to true for MR jobs
	    
	    scan.addColumn(Bytes.toBytes("mtdt"), Bytes.toBytes("_hr_"));
	    scan.addColumn(Bytes.toBytes("mtdt"), Bytes.toBytes("_tr_"));
	    scan.addColumn(Bytes.toBytes("mtdt"), Bytes.toBytes("_ur_"));
    
	    
    
	    Path outpath = new Path(
	            "/user/crawler/cengiz/ttt");
	    job.setOutputFormatClass(TextOutputFormat.class);
	
	    job.setOutputKeyClass(CompositeWritable.class);
	    job.setOutputValueClass(NullWritable.class);
	   
	    FileOutputFormat.setOutputPath(job, outpath);
	    
	    String tableName = TABLE_NAME;
	    
		TableMapReduceUtil.initTableMapperJob(
	      tableName,        // input HBase table name
	      scan,             // Scan instance to control CF and attribute selection
	      ReadFromHBaseMapper.class,   // mapper
	      CompositeWritable.class,             // mapper output key
	      DoubleWritable.class,             // mapper output value
	      job);
	
		job.setReducerClass(ReadFromHBaseReducer.class);
		//job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
		
	
		//job.setNumReduceTasks(1);
	    boolean b = job.waitForCompletion(true);
	    if (!b) {
	      throw new IOException("error with job!");
	    }
      
    }
    
    public static class ReadFromHBaseMapper extends
    TableMapper<CompositeWritable, DoubleWritable> {
    	List<Result> resultArr = new ArrayList<Result>();
    	public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
    		byte[] hrBytes = value.getValue(Bytes.toBytes("mtdt"), Bytes.toBytes("_hr_"));
    		byte[] trBytes = value.getValue(Bytes.toBytes("mtdt"), Bytes.toBytes("_tr_"));
    		byte[] urBytes = value.getValue(Bytes.toBytes("mtdt"), Bytes.toBytes("_ur_"));
    		
    		double totalScore, hrScore = 0, urScore = 0, trScore = 0;
    		
    		resultArr.add(value);
    		
    		String host = Bytes.toString(row.get());
    		
    		if (hrBytes != null){
         	    hrScore = Double.parseDouble(Double.toString(Bytes.toDouble(hrBytes)));
                host = unreverseUrl(host);       
            }
    		
    		if (urBytes != null){
         	    urScore = Double.parseDouble(Double.toString(Bytes.toDouble(urBytes)));
                host = unreverseUrl(host);              
            }
    		
    		if (trBytes != null){
         	    trScore = Double.parseDouble(Double.toString(Bytes.toDouble(trBytes)));
                //host = unreverseUrl(host);              
            }
    		totalScore = hrScore + urScore + trScore;
    		context.write(new CompositeWritable(new Text(host), new DoubleWritable(hrScore), new DoubleWritable(trScore), new DoubleWritable(urScore)), new DoubleWritable(totalScore));
    	}
    	
    	public static String unreverseUrl(String url){
    		String[] pieces = StringUtils.split(url, '.');
    		StringBuilder buf = new StringBuilder(url.length());
    		for(int i = pieces.length - 1; i >= 0; i--){
    			buf.append(pieces[i]);
    			if(i != 0){
    				buf.append('.');
    			}
    		}
    		return buf.toString().startsWith("www.") ? buf.toString().substring(4) : buf.toString();
    	}
    }
    
    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(DoubleWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleWritable key1 = (DoubleWritable) w1;
            DoubleWritable key2 = (DoubleWritable) w2;   
            return -1 * key1.compareTo(key2);
        }
    }
    
    public static class ReadFromHBaseReducer extends
	    Reducer<CompositeWritable, NullWritable, CompositeWritable, NullWritable> {
	
	    @Override
	    public void reduce(CompositeWritable cw, Iterable<NullWritable> values, Context context)
	        throws IOException, InterruptedException {
	    	context.write(cw, null);
	    }
    }
}



