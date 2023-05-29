package it.unipi.kurapika;

import org.apache.hadoop.mapreduce.Reducer;
import it.unipi.kurapika.utilities.*;

import java.io.IOException;

public class KmeansCombiner extends Reducer<Centroid, Point, Centroid, Point>{

    // for each subgroup of a cluster calculate partial sum
    @Override
    protected void reduce(Centroid key, Iterable<Point> points, Context context) throws IOException, InterruptedException {
        Point result = new Point();	// new point standing for partial sum
        
        for (Point point : points) {
            result.sum(point);		// add points to result
        }
        
        context.write(key, result);	// write output record (key: cluster, value: partial sum)
	}
}
