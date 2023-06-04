package it.unipi.kurapika;

import org.apache.hadoop.mapreduce.Reducer;
import it.unipi.kurapika.utilities.*;

import java.io.IOException;

public class KmeansCombiner extends Reducer<Centroid, Point, Centroid, Point>{

    // for each subgroup of a cluster calculate partial sum
    @Override
    protected void reduce(Centroid key, Iterable<Point> points, Context context) throws IOException, InterruptedException {
    	
        Point partialSum = new Point();	// new point standing for all points belonging to the same cluster and on the same machine
        
        for (Point point : points) {
            partialSum.sum(point);		// add points to partial sum
        }
        
        context.write(key, partialSum);	// write output record (key: cluster, value: partial sum)
	}
}
