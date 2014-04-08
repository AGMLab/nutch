package org.apache.nutch.comparison;

import java.io.BufferedReader;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Kendall {
	/**
	 * @author sertug
	 * @author aykut
	 * @author cengiz
	 */
	
	LinkedHashMap<String, Integer>list1;
	LinkedHashMap<String, Integer>list2;
	LinkedHashMap<String, Integer> alexa = null;
	LinkedHashMap<String, Integer> list;
	static LinkedHashMap<String, Double> dataLine = null;
	
	public Kendall() throws NumberFormatException, IOException {
		super();
		this.list1 = new LinkedHashMap<String, Integer>();
		this.list2 = new LinkedHashMap<String, Integer>();
		if(alexa == null){
			alexa = readFile("/user/crawler/cengiz/domainseed.csv");
		}
	}

	public static void main(String[] args) throws NumberFormatException, IOException, ClassNotFoundException, InterruptedException{
		//start(new double[]{0.0, 1.0, 0.0}, args);
	}
	
	@SuppressWarnings("deprecation")
	public double start(double[] coeffs, String[] args) throws NumberFormatException, IOException, ClassNotFoundException, InterruptedException {
		System.out.println(args);
		
		long start = System.currentTimeMillis();
		
		
		
		Configuration conf2 = new Configuration();
        conf2.set("mapred.job.priority", "VERY_HIGH");
        long end = System.currentTimeMillis();
        System.out.println("before try: " + (end - start));
		try {
			start = System.currentTimeMillis();
			Path pa = new Path("/user/crawler/cengiz/ttt/");
	 	    FileSystem fi = FileSystem.get(pa.toUri(), conf2);
		   
	 	    //BufferedReader bufferedReader2 = new BufferedReader(new InputStreamReader(fileSystem.open(pa)));
	 	    //File f = new File(pa.getName());

	 	    System.out.println("exists " + fi.exists(pa) + " isDirect " + fi.isDirectory(pa));
			if (!(fi.exists(pa) && fi.isDirectory(pa))) {
				System.out.println(args);
				ReadFromHBase.start(args);
			}
			end = System.currentTimeMillis();
	        System.out.println("readfromhbase try: " + (end - start));
	        start = System.currentTimeMillis();
			list = readScoreFile("/user/crawler/cengiz/ttt/part-r-00000", coeffs);
			System.out.println("tausize " + list.size());
			
			arrayListsFromMaps();
			end = System.currentTimeMillis();
	        System.out.println("readfromhdfs try: " + (end - start));
			
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		double result = getDistance(alexa, list);
		System.out.println(result);
		return result;
	}
	
	public static double getDistance(LinkedHashMap<String, Integer> list12, LinkedHashMap<String, Integer> list22){
		int concordantCounter = 0;
		int discordantCounter = 0;
		double tauDistance = 0.0d;
		
		System.out.println(list12.size());
		System.out.println(list22.size());
		
//		 for (int i=0; i<list1.size();i++){
//			 if((list1.get(i) - 1) / 100 != (list2.get(i) - 1) / 100){
//				discordantCounter++;
//			 }
//			 else
//				concordantCounter++;
//		 }
				
		List<String> keys = new ArrayList<String>(list12.keySet());
		List<String> keys2 = new ArrayList<String>(list22.keySet());
		for (int i = 0; i < keys.size() - 1; i++){
			for(int j = i + 1; j < keys2.size(); j++){
				//if(list1.get(i) <= 100)
				//System.out.println(list12.get(i) + " " + list12.get(j));
				if(list12.get(keys.get(i)) < list12.get(keys.get(j)) ^ list22.get(keys.get(i)) < list22.get(keys.get(j)))
					discordantCounter++;
				else
					concordantCounter++;
			}
		}
		
		System.out.println("uyumsuz: " + discordantCounter +  " uyumlu: " + concordantCounter);
		tauDistance= (1.0 * (concordantCounter-discordantCounter))/(concordantCounter+discordantCounter);
		return tauDistance;
	}
	
	public static LinkedHashMap<String, Integer> readFile(String filePath) throws NumberFormatException, IOException{
		Configuration conf2 = new Configuration();
		//conf2.set("fs.default.name", "file:///");
		Path pa = new Path(filePath);
 	    FileSystem fileSystem = FileSystem.get(pa.toUri(), conf2);
	    BufferedReader bufferedReader2 = new BufferedReader(new InputStreamReader(fileSystem.open(pa)));
		
		
		String line=null;
		LinkedHashMap<String, Integer> dataLine = new LinkedHashMap<String, Integer>();
		line = bufferedReader2.readLine();
		while ((line = bufferedReader2.readLine()) != null) {
            String[] splitted = line.split(",");
            dataLine.put(splitted[1], Integer.valueOf(splitted[2]));
        }
		
		if (bufferedReader2 != null)
			bufferedReader2.close();
		
		return dataLine;
	}
	
	public static LinkedHashMap<String, Integer> readScoreFile(String filePath, double[] arr) throws NumberFormatException, IOException{
		Configuration conf2 = new Configuration();
		//BufferedReader stream = new BufferedReader(new FileReader(filePath));
		Path pa = new Path(filePath);
 	    FileSystem fileSystem = FileSystem.get(pa.toUri(), conf2);
	    BufferedReader bufferedReader2 = new BufferedReader(new InputStreamReader(fileSystem.open(pa)));
		double hrFactor = arr[0];
		double urFactor = arr[1];
		double trFactor = arr[2];
		
		String line=null;
		if(dataLine == null){
			dataLine = new LinkedHashMap<String, Double>();
			while ((line = bufferedReader2.readLine()) != null) {
	            String[] splitted = line.split("\t");
	            double totalScore = Double.valueOf(splitted[1]) * hrFactor + Double.valueOf(splitted[2]) * urFactor +
	            		Double.valueOf(splitted[3]) * trFactor;
	            dataLine.put(splitted[0], totalScore);
	        }
			
			if (bufferedReader2 != null)
				bufferedReader2.close();
		}
		System.out.println("readscorefile girdi");
		//mr
		
		
		 List<Map.Entry<String, Double>> entries = new ArrayList<Map.Entry<String, Double>>(dataLine.entrySet());
	       Collections.sort(entries, new Comparator<Map.Entry<String, Double>>() {
	           @Override
	           public int compare(Map.Entry<String, Double> entry1, Map.Entry<String, Double> entry2) {
	               return entry1.getValue().compareTo(entry2.getValue());
	           }
	       });
		
	    LinkedHashMap<String, Integer> result = new LinkedHashMap<String, Integer>();
	    int count = 1;
	    for(Map.Entry<String, Double> me: entries){
	    	result.put(me.getKey(), count++);
	    }
	    //mr
	    
	    System.out.println(" readscorefile result size " + result.size());
		return result;
	}
	
	public void arrayListsFromMaps(){
		LinkedHashMap<String, Integer> neuMap = new LinkedHashMap<String, Integer>();
		LinkedHashMap<String, Integer> neuMap2 = new LinkedHashMap<String, Integer>();
		
		for (Map.Entry<String, Integer> entry : alexa.entrySet()) {
			String host = entry.getKey();
			if(list.containsKey(host)){
				neuMap.put(host, alexa.get(host));
				neuMap2.put(host, list.get(host));
			}			
		}
	
		alexa = neuMap;
		list = neuMap2;
	}
}