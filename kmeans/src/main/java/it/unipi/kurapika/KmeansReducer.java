package it.unipi.kurapika;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import it.unipi.kurapika.utilities.*;

public class KmeansReducer extends Reducer<Centroid, Point, Text, Text>{
	
	public static enum Counter {
		// Global counter: it gets incremented every time new centroids are more than epsilon distant from previous centroids
		CONVERGED
	}
	
	private final List<Centroid> centers = new ArrayList<>();  // list containing new centroids
	
	private Double epsilon = 0.;		// convergence parameter 
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);
	    Configuration conf = context.getConfiguration();
	    epsilon = conf.getDouble("epsilon", 0.001);	// initialize convergence parameter with value in configuration file
	}
	
	// for each cluster calculate new centroids
    @Override
    protected void reduce(Centroid key, Iterable<Point> partialSums, Context context) throws IOException, InterruptedException {
    	
    	Centroid newKey = new Centroid();			// new centroid
    	Text label = new Text();
    	
    	for (Point point : partialSums) {			// summation of partial sums 
    	    newKey.getPoint().sum(point);	
    	}
    	newKey.getPoint().compress();				// divide for number of points in cluster
    	newKey.setIndex(key);						// assign old centroid's index to new centroid
   
    	centers.add(newKey);						// add new centroid to new centroids list
    	label.set(newKey.toString());				// get its coordinates in Text format
    	
    	context.write(newKey.getLabel(), label);			// write output record (key: centroid, value: null)
    	
    	// calculate distance between new centroid and old centroid
    	double distance = key.getPoint().getDistance(newKey.getPoint());
    	if (distance > epsilon) {				// if distance is greater than epsilon
    	    context.getCounter(Counter.CONVERGED).increment(1);	// increment global counter
    	}
    }
    	
	// store new coordinates
    @Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		int numberClusters = conf.getInt("k", 4);
		String[] result = new String[numberClusters];
		
		int index = 0;
		for(Centroid newCentroid : centers) {
			result[index] = newCentroid.getPoint().toString();
		}
		conf.setStrings("centroids", result);
	}
    
}
