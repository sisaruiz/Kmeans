package it.unipi.kurapika.utilities;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;

public class Utility {

	public static Point[] centroidsInit(Configuration conf, String pathString, int k, int dataSetSize) 
		      throws IOException {
		    	Point[] points = new Point[k];
		    	
		        //Create a sorted list of positions without duplicates
		        //Positions are the line index of the random selected centroids
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
		        
		        //File reading utils
		        Path dataPath = new Path(pathString);
		        FileSystem hdfs = FileSystem.get(conf);
		        FSDataInputStream in = hdfs.open(dataPath);
		        BufferedReader br = new BufferedReader(new InputStreamReader(in));

		        //Get centroids from the file
		        int row = 0;
		        int i = 0;
		        int position;
		        while(i < positions.size()) {
		            position = positions.get(i);
		            String point = br.readLine();
		            if(row == position) {    
		                points[i] = new Point();
		                points[i].parse(point);
		                i++;
		            }
		            row++;
		        }   
		        br.close();
		        
		    	return points;
		    }
	
	public static void writeExampleCenters(Configuration conf, Path center, Point[] points) throws IOException {
		try (SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf, SequenceFile.Writer.file(center) , 
				SequenceFile.Writer.keyClass(Centroid.class), SequenceFile.Writer.valueClass(IntWritable.class))) {
			final IntWritable value = new IntWritable(0);
			for (Point point : points) {
				centerWriter.append(point, value);
			}
			
		}
	}

}
