package org.apache.nutch.scoring;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.NormalDistribution;
import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.util.TableUtil;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: aykut
 * Date: 9/5/13
 * Time: 3:39 PM
 * To change this template use File | Settings | File Templates.
 */

public class UsageRankNormalization {
    private static String TABLE_NAME = "host";

    private static Double getMean(Double[] logVals){
        Double sum = 0.0;
        for (Double val : logVals){
            sum += val;
        }
        return sum / logVals.length;
    }

    private static Double getVariance(Double[] logVals){
        Double mean = getMean(logVals);
        Double sum = 0.0;
        for(Double val : logVals){
             sum += (mean - val) * (mean - val);
        }
        return sum / logVals.length;
    }

    private static int countLine(File file) throws IOException {
        int lines = 0;
        LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(file));
        lineNumberReader.skip(Long.MAX_VALUE);
        lines = lineNumberReader.getLineNumber();
        lineNumberReader.close();
        return lines;
    }

    public static Double stdDev(Double[] logVals){
        return Math.sqrt(getVariance(logVals));
    }

    public static void normalizeUsageValues(String[] args, int scale){
    	String inputFile=null;
    	boolean hdfs = true;
    	for (int i = 0; i < args.length; i++){
        	if (args[i].equals("-f")) {
        		inputFile = args[i+1];
        		hdfs = false;
        	}
        	if(args[i].equals("-p")){
        		TABLE_NAME=args[i+1]+"_"+TABLE_NAME;
        	}    	
        	if (args[i].equals("-h")) {
        		inputFile = args[i+1];
        	}
        }
    	
        BufferedReader bufferedReader = null;
        Double sumOfLog = 0.0d;
        Double logAvg = 0.0d;
        Double stdDevVal = 0.0d;
        Double newValue = 1.0d;
        int numberOfLines = 0;
        String[] hosts;
        Double[] logVals;
        String reversedUrl;
            
        try {        
        	@SuppressWarnings("deprecation")
			Configuration conf = new HBaseConfiguration();
            HTable table = new HTable(conf, TABLE_NAME);
            
//            if (conf == null){
//                System.exit(-1);
//            } 
            Configuration conf2 = null;
            try {
            	   Path p = new Path(inputFile);
//            	   Path path = new Path("hdfs://user/cengiz/data/deneme.txt");
//            	   conf.addResource("/home/hadoop/conf/core-site.xml");
            	   conf2 = new Configuration();
            	   //local setting
            	   //conf.set("fs.default.name", "hdfs://10.6.149.119:8085:9000");
            	   conf2.set("mapred.job.priority", "VERY_HIGH");
            	   //conf.set("hbase.zookeeper.quorum","host-10-6-149-119");
            	   if(!hdfs)
            		   conf2.set("fs.default.name", "file:///");
            	   FileSystem fileSystem = FileSystem.get(conf2);
//            	   FileSystem fileSystem = FileSystem.get(org.apache.hadoop.conf.Configured.getConf());
            	   BufferedReader bufferedReader2 = new BufferedReader(new InputStreamReader(fileSystem.open(p)));
            	   String line = bufferedReader2.readLine();
            	   //numberOfLines++;
            	   while (line != null) {
            	    System.out.println(line);
            	    line = bufferedReader2.readLine();
            	    numberOfLines++;
            	   }
            	  } catch (IOException e) {
            	   e.printStackTrace();
            	  }
//            bufferedReader = new BufferedReader(new FileReader(inputFile));
           String line = null;
            //ArrayList<Double> logValList = new ArrayList<Double>();

            hosts = new String[numberOfLines];
            logVals = new Double[numberOfLines];
             int i = 0;

      	   Path pa = new Path(inputFile);
//      	   Path path = new Path("hdfs://user/cengiz/data/deneme.txt");
//      	   conf.addResource("/home/hadoop/conf/core-site.xml");
      	   //local setting
      	   //conf2.addResource(new Path("/home/cengiz/hadoop/conf/core-site.xml"));
      	   FileSystem fileSystem = FileSystem.get(conf2);
             BufferedReader bufferedReader2 = new BufferedReader(new InputStreamReader(fileSystem.open(pa)));
            while ( (line = bufferedReader2.readLine()) != null){
                String[] cols = line.split("\t");
                Double logValue = Math.log(Integer.parseInt(cols[1]));
                logVals[i] = logValue;
//                hosts[i] = reverseHost(cols[0]);
                reversedUrl = TableUtil.reverseUrl("http://" + cols[0]);
                hosts[i] = TableUtil.getReversedHost(reversedUrl);
                sumOfLog += logValue;
                i++;
            }
            
            logAvg = sumOfLog / logVals.length;
            stdDevVal = stdDev(logVals);

            if (stdDevVal == 0.0d) {
                stdDevVal = 1e-10;
            }

            for (i = 0; i < hosts.length; i++){
                NormalDistribution distribution = new NormalDistributionImpl(logAvg, stdDevVal);
                newValue = distribution.cumulativeProbability(logVals[i]) * scale;
                Put p = new Put(Bytes.toBytes(hosts[i]));
                System.out.println("host: " + hosts[i]  + "\tvalue: "+ String.valueOf(newValue));
//                System.out.println("string to byte value: "+Bytes.toBytes(String.valueOf(newValue)));
//                System.out.println("double to byte value: "+Bytes.toBytes(newValue));
                p.add(Bytes.toBytes("mtdt"), Bytes.toBytes("_ur_"), Bytes.toBytes(newValue));
                table.put(p);
            }
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (MathException e) {
            e.printStackTrace();
        } finally {
            if (bufferedReader != null)
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }
    
    /**
     * @param host
     * 
     * example:
     * http://meb.gov.tr/
     * http://milliyet.tv/
     * http://acunn.com/
     * @return reversedHost
     * 
     * example:
     * tr.gov.meb.www
     * tv.milliyet.www
     * com.acunn.www
     */
//    public static String reverseHost(String host){
//    	host= host.substring(host.indexOf("/")+2,host.lastIndexOf("/"));
//    	String[] pieces = StringUtils.split(host, '.');
//    	StringBuilder buf = new StringBuilder(host.length());
//    	for(int i = pieces.length - 1; i >= 0; i--){
//    		buf.append(pieces[i]);
//    		if(i != 0){
//    			buf.append('.');
//    		}
//    	}
////    	buf.append(".www");
//    	return buf.toString();
//    }

    public static void main(String[] args){
        if (args.length < 1){
            System.err.println("Usage: UsageRankNormalization -f <host_file> -p <output_table_prefix>");
            return;
        }
        normalizeUsageValues(args, 10);
    }
}
