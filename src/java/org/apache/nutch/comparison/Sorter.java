package org.apache.nutch.comparison;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;



import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author sertug
 * @author aykut
 * @author cengiz
 */

public class Sorter {

   public static final String TABLE_NAME = "host";
   private static LinkedHashMap<String, Double> hrmap = new LinkedHashMap<String, Double>();
   private static LinkedHashMap<String, Double> urmap = new LinkedHashMap<String, Double>();
   private static LinkedHashMap<String, Double> trmap = new LinkedHashMap<String, Double>();
   private static List<Map.Entry<String, Double>> entries = null;
   private static List<Result> resultArr = new ArrayList<Result>();

   public static  List<Map.Entry<String, Double>> scaleScores(Double hrFactor, Double urFactor, Double trFactor) throws IOException {
	   if(resultArr.size() ==  0){
	       Configuration config = HBaseConfiguration.create();
	       //config.set("hbase.zookeeper.quorum","hdpzk01.damla.local,hdpzk02.damla.local,hdpzk03.damla.local");
	       
	       HTable table = new HTable(config, TABLE_NAME);
	       Scan s = new Scan();
	
	       s.addColumn(Bytes.toBytes("mtdt"), Bytes.toBytes("_hr_"));
	       s.addColumn(Bytes.toBytes("mtdt"), Bytes.toBytes("_tr_"));
	       s.addColumn(Bytes.toBytes("mtdt"), Bytes.toBytes("_ur_"));
	
	       ResultScanner scanner = table.getScanner(s);
	       try {
	           for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
	               resultArr.add(rr);
	           }
	       } finally {
	           scanner.close();
	       }
	   } 
	   
	   hrmap = new LinkedHashMap<String, Double>();
	   urmap = new LinkedHashMap<String, Double>();
	   trmap = new LinkedHashMap<String, Double>();
	   
	   
       for(Result rr: resultArr){
    	   String host = Bytes.toString(rr.getRow());
           
           double hrScore = 0.0d;
           double trScore = 0.0d;
           double urScore = 0.0d;
           double totalScore = 0.0d;

           byte[] hrBytes = rr.getValue(Bytes.toBytes("mtdt"), Bytes.toBytes("_hr_"));
           byte[] trBytes = rr.getValue(Bytes.toBytes("mtdt"), Bytes.toBytes("_tr_"));
           byte[] urBytes = rr.getValue(Bytes.toBytes("mtdt"), Bytes.toBytes("_ur_"));

           if (hrBytes != null){
        	   hrScore = Double.parseDouble(Double.toString(Bytes.toDouble(hrBytes)));
               hrScore *= hrFactor;   
               host = unreverseUrl(host);
               if(hrmap.containsKey(host) && hrmap.get(host) > hrScore){
            	   hrmap.put(host, hrmap.get(host));
            	   
               } else {
            	   hrmap.put(host, hrScore);
               }
           }

           if (trBytes != null){
               trScore = Double.parseDouble(Double.toString(Bytes.toDouble(trBytes)));
               trScore *= trFactor;
               if(trmap.containsKey(host) && trmap.get(host) > trScore){
            	   trmap.put(host, trmap.get(host));
            	   
               } else {
            	   trmap.put(host, trScore);
               }
           }

           if (urBytes != null){
               urScore = trScore = Double.parseDouble(Double.toString(Bytes.toDouble(urBytes)));
               urScore *= urFactor;
               if(urmap.containsKey(host) && urmap.get(host) > urScore){
            	   urmap.put(host, urmap.get(host));
               } else {
            	   urmap.put(host, urScore);
               }
           }
       }
       
//       for (Map.Entry<String, Double> entry : hrmap.entrySet()) {
//    	   for (Map.Entry<String, Double> entry2 : urmap.entrySet()) {
//    		   if(entry2.getKey().toString().equals(entry.getKey().toString())){
//    			   hrmap.put(entry.getKey().toString(), (double)entry2.getValue() + (double)entry.getValue());
//    		   }
//           }
//	   }
       
       for (Map.Entry<String, Double> entry : hrmap.entrySet()) {
//    	   System.out.println(entry.getKey() + " " + entry.getValue());
    	   if(urmap.containsKey(entry.getKey())){
    		   hrmap.put(entry.getKey().toString(), (double)urmap.get(entry.getKey()) + (double)entry.getValue());
    	   }
    	   if(trmap.containsKey(entry.getKey())){
    		   hrmap.put(entry.getKey().toString(), (double)trmap.get(entry.getKey()) + (double)entry.getValue());
    	   }
//    	   System.out.println(entry.getKey() + " " + entry.getValue());
       }
       
       
          
//       for (Map.Entry<String, Double> entry : hrmap.entrySet()) {
//    	   for (Map.Entry<String, Double> entry2 : trmap.entrySet()) {
//    		   if(entry2.getKey().toString().equals(entry.getKey().toString())){
//    			   hrmap.put(entry.getKey().toString(), (double)entry2.getValue() + (double)entry.getValue());
//    		   }
//           }
//	   }
       

       entries = new ArrayList<Map.Entry<String, Double>>(hrmap.entrySet());
       Collections.sort(entries, new Comparator<Map.Entry<String, Double>>() {
           @Override
           public int compare(Map.Entry<String, Double> entry1, Map.Entry<String, Double> entry2) {
               return entry2.getValue().compareTo(entry1.getValue());
           }
       });
       double count = 1;
       for (Map.Entry<String, Double> entry : entries) {
    		hrmap.put(entry.getKey().toString(), count);
    		//System.out.println(entry.getKey() + " u " + entry.getValue());
    		count++;
	   }
       return  entries;
   }

   public static List<Map.Entry<String, Double>> sort(double[] coeffs) throws IOException{
	   //if(entries != null)
		   //return entries;
	   List<Entry<String, Double>> sScores = scaleScores(coeffs[0], coeffs[1], coeffs[2]);
       return sScores;
   }

   public static void main(String[] args){
	   double[] coeffs = {0.33, 0.33, 0.33};
       try {
           scaleScores(coeffs[0], coeffs[1], coeffs[2]);
       } catch (IOException e) {
           e.printStackTrace();
       }
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