package org.apache.nutch.tools;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: aykut
 * Date: 9/2/13
 * Time: 11:35 AM
 * To change this template use File | Settings | File Templates.
 */
public class TrustFlagLoader {

    private static final String TABLE_NAME = "host";


        public static void writeHostListToHBase(String path) throws IOException {
            File inputFile = new File(path);
            BufferedReader bufferedReader = null;
            Configuration conf = HBaseConfiguration.create();
            HTable table = new HTable(conf, TABLE_NAME);


            if (conf == null){
                return;
            }

            try {
                bufferedReader = new BufferedReader(new FileReader(inputFile));
                String line = null;
                String reversedHost=null;

                while ( (line = bufferedReader.readLine()) != null){
                   String[] cols = line.split("\t");
                   reversedHost=reverseHost(cols[0]);
                   System.out.println("reversedHost: " + reversedHost);
                    Put p = new Put(Bytes.toBytes(reversedHost));

                   p.add(Bytes.toBytes("mtdt"), Bytes.toBytes("_tf_"), Bytes.toBytes("1"));
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
        public static String reverseHost(String host){
        	host= host.substring(7);
        	String[] pieces = StringUtils.split(host, '.');
        	StringBuilder buf = new StringBuilder(host.length());
        	for(int i = pieces.length - 1; i >= 0; i--){
        		buf.append(pieces[i]);
        		if(i != 0){
        			buf.append('.');
        		}
        	}
        	buf.append(".www");
        	return buf.toString();
        }

        public static void main(String[] args){

            if (args.length < 1){
                System.err.println("Usage: TrustFlagLoader <host_file>");
                return;
            }

            try {
                writeHostListToHBase(args[0]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

}