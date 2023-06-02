package it.unipi.kurapika;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import it.unipi.kurapika.utilities.*;

public class KmeansReducer extends Reducer<Centroid, Point, Centroid, NullWritable>{
	
	public static enum Counter {
		// Global counter: it gets incremented every time new centroids are more than epsilon distant from previous centroids
		CONVERGED
	}
	
	private final List<Centroid> centers = new ArrayList<>();  // list containing new centroids
	
	private Double epsilon = 0.;		// convergence parameter 
	
	@Override
	protected void setup(Context context) {
	    Configuration conf = context.getConfiguration();
	    epsilon = conf.getDouble("epsilon", 0.0001);	// initialize convergence parameter with value in configuration file
	}
	
	// for each cluster calculate new centroids
        @Override
        protected void reduce(Centroid key, Iterable<Point> partialSums, Context context) throws IOException, InterruptedException {
    	
    	    Centroid newKey = new Centroid();			// new centroid
    	
    	    for (Point point : partialSums) {			// summation of partial sums 
    		newKey.getPoint().sum(point);	
    	    }
    	    newKey.getPoint().compress();			// divide for number of points in cluster
    	    newKey.setIndex(key);				// assign old centroid's index to new centroid
    	
    	    centers.add(newKey);				// add new centroid to new centroids list
	    context.write(newKey, NullWritable.get());		// write output record (key: centroid, value: null)
    	
	    // calculate distance between new centroid and old centroid
	    double distance = key.getPoint().getDistance(newKey.getPoint());
    	    if (distance > epsilon) {				// if distance is greater than epsilon
    		context.getCounter(Counter.CONVERGED).increment(1);	// increment global counter
    	    }
        }
    	
	// write new centroids in sequence file
    	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		Path outPath = new Path(conf.get("centroids.path", "centroids.txt"));		// get path of centroids sequence file
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);				// if path exists delete it
		try (SequenceFile.Writer out = SequenceFile.createWriter(conf, SequenceFile.Writer.file(outPath),
				SequenceFile.Writer.keyClass(Centroid.class))) {
			final IntWritable value = new IntWritable(0);
			for (Centroid center : centers) {	
				out.append(center, value);		// write new centroids in sequence file
			}
		}
	}
    
}
