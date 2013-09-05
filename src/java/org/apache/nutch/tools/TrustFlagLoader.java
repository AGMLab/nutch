package org.apache.nutch.tools;


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

                while ( (line = bufferedReader.readLine()) != null){
                   String[] cols = line.split("\t");
                   System.out.println("HOST: " + cols[0]);
                    Put p = new Put(Bytes.toBytes(cols[1]));
                   System.out.println("VISIT: " + cols[2]);

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