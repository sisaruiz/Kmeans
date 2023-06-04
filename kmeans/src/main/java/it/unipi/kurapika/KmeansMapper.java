package it.unipi.kurapika;

import java.io.IOException;
import java.util.List;
import java.lang.*;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.unipi.kurapika.utilities.*;

public class KmeansMapper extends Mapper<LongWritable, Text, Centroid, Point>{
	
	private Point point = new Point();						// datapoint to be examined
	private List<Centroid> centroids = new ArrayList<>();		// list of centroids

	// for each task first initialize centroids
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		Path centroidsPath = new Path(conf.get("centroids.path", "centroids.seq")); 	// path containing sequence file to read

		try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centroidsPath))) {
			
			Point key = new Point();
			int i = 0;

			while (reader.next(key)) {			// iterate over records
				Centroid center = new Centroid(String.valueOf(i), key);	// create new centroid 
				centroids.add(center);			// add new Centroid to list
				i++;
			}
		}
	}
	
	// for each record (datapoint) assign closest centroid
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		Centroid cluster = new Centroid();			// centroid to be assigned
		point.parse(value.toString());			// convert data value to Point
		
		double minimumDistance = Double.MAX_VALUE;	// assign maximum value as default minimum distance
		
		for(int i=0; i<centroids.size(); i++) {		// for each centroid
			double distance = point.getDistance(centroids.get(i).getPoint());	// calculate distance from point to centroid
			if (distance < minimumDistance) {	// if distance is shorter than the previous ones
                		cluster = centroids.get(i);	// assign new centroid to point
                		minimumDistance = distance;	// update minimumDistance with new value
            		}
		}
		
		context.write(cluster, point);			// write output record (key: cluster, value: point)
	}
	
}
