package it.unipi.kurapika.utilities;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class Utility {

	// generate random k centroids
	public static void generateCentroids(Configuration conf, Path inPath, int k, int dataSetSize) throws IOException {
		
		// get random indexes
		List<Integer> positions = new ArrayList<Integer>();
		Random random = new Random();
		int pos;
		
		while(positions.size() < k) {
			pos = random.nextInt(dataSetSize);
		    if(!positions.contains(pos)) {
		    	positions.add(pos);
		    }
		}
		Collections.sort(positions);
		        
		// get elements
		FileSystem hdfs = FileSystem.get(conf);
		FSDataInputStream in = hdfs.open(inPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		int line = 0;
		int i = 0;
		int position;
		String[] centroids = new String[k];
		while(i < positions.size()) {
			position = positions.get(i);
		    String point = br.readLine();
		    if(line == position) {    
		    	centroids[i] = point;
		        i++;
		    }
		    line++;
		}
		br.close();
		// save centroids
		conf.setStrings("centroids", centroids);
	}
	
	public static void writeOutputFile(Configuration conf, Path outpath) throws IOException {
		
		FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream dos = hdfs.create(outpath, true);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));

        // get centroids
        String[] lines = conf.getStrings("centroids");
        
        // print centroids in output file
        for(int i = 0; i < lines.length; i++) {
        	br.write(lines[i]);
        	br.newLine();
        }
        br.close();
        hdfs.close();
	}
}
