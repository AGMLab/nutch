package org.apache.nutch.tools;


import org.apache.commons.lang.StringUtils;
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
 * Date: 9/2/13
 * Time: 11:35 AM
 * To change this template use File | Settings | File Templates.
 */
public class TrustFlagLoader {
    private static String TABLE_NAME = "host";

    public static void writeHostListToHBase(String[] args) throws IOException {
        String inputFile = null;
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
        @SuppressWarnings("deprecation")
		Configuration conf = new HBaseConfiguration();
        HTable table = new HTable(conf, TABLE_NAME);
        System.out.println("a");
        Configuration conf2 = new Configuration();
        System.out.println("b");
        conf2.set("mapred.job.priority", "VERY_HIGH");
        System.out.println("c");
        FileSystem fileSystem = null;
        System.out.println("d");
        BufferedReader bufferedReader2 = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(inputFile));
            String line = null;
            String reversedHost=null;
            String reversedUrl=null;
            String trustFlag="1";
            
            
     	    if(!hdfs){
     		    conf2.set("fs.default.name", "file:///");
      	    }
     	    Path pa = new Path(inputFile);
     	    fileSystem = FileSystem.get(pa.toUri(), conf2);
 	        bufferedReader2 = new BufferedReader(new InputStreamReader(fileSystem.open(pa)));
            
            while ( (line = bufferedReader2.readLine()) != null){
               String[] cols = line.split("\t");
               reversedUrl = TableUtil.reverseUrl("http://" + cols[0]);
               reversedHost = TableUtil.getReversedHost(reversedUrl);
               Put p = new Put(Bytes.toBytes(reversedHost));
               p.add(Bytes.toBytes("mtdt"), Bytes.toBytes("_tf_"), Bytes.toBytes(trustFlag));
               table.put(p);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
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
     * http://meb.gov.tr
     * http://milliyet.tv
     * http://acunn.com
     * @return reversedHost
     * 
     * example:
     * tr.gov.meb.www
     * tv.milliyet.www
     * com.acunn.www
     */
//        public static String reverseHost(String host){
//        	host= host.substring(7);
//        	String[] pieces = StringUtils.split(host, '.');
//        	StringBuilder buf = new StringBuilder(host.length());
//        	for(int i = pieces.length - 1; i >= 0; i--){
//        		buf.append(pieces[i]);
//        		if(i != 0){
//        			buf.append('.');
//        		}
//        	}
//        	buf.append(".www");
//        	return buf.toString();
//        }

    public static void main(String[] args){
        if (args.length < 1){
            System.err.println("Usage: TrustFlagLoader -f <host_file> -p <output_table_prefix>");
            return;
        }
        try {
        	System.out.println("zor");
            writeHostListToHBase(args);
            System.out.println("cok");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}