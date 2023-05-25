package it.unipi.kurapika;

import org.apache.hadoop.mapreduce.Reducer;
import it.unipi.kurapika.utilities.*;

import java.io.IOException;

public class KmeansCombiner extends Reducer<Centroid, Point, Centroid, Point>{

	@Override
    protected void reduce(Centroid key, Iterable<Point> points, Context context) throws IOException, InterruptedException {
        Point result = new Point();
        
        for (Point point : points) {
            result.sum(point);
        }
        
        context.write(key, result);
	}
}
