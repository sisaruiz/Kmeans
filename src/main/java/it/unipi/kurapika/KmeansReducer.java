package it.unipi.kurapika;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import it.unipi.kurapika.utilities.*;

public class KmeansReducer extends Reducer<Centroid, Point, Centroid, NullWritable>{
	
	public static enum Counter {
		CONVERGED
	}
	
	private Double epsilon = 0.;
	
	@Override
	    protected void setup(Context context) {
	        Configuration conf = context.getConfiguration();
	        epsilon = conf.getDouble("epsilon", 0.0001);
	    }
	
    @Override
    protected void reduce(Centroid key, Iterable<Point> partialSums, Context context) throws IOException, InterruptedException {
    	
    	Centroid newKey = new Centroid();
    	
    	for (Point point : partialSums) {
    		newKey.getPoint().sum(point);
    	}
    	newKey.getPoint().compress();
    	newKey.setIndex(key);
    	
    	context.write(newKey, NullWritable.get());
    	
    	if (key.getPoint().getDistance(newKey.getPoint()) > epsilon) {
    		context.getCounter(Counter.CONVERGED).increment(1);
    	}
    }
    
}
