package it.unipi.kurapika;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.unipi.kurapika.utilities.*;

public class KmeansMapper extends Mapper<LongWritable, Text, Centroid, Point>{
	
	private Point point = new Point();							// datapoint to be examined
	private List<Centroid> centroids = new ArrayList<>();		// list of centroids

	// for each task first initialize centroids
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);
		Configuration conf = context.getConfiguration();
		String[] lines = conf.getStrings("centroids");
        
        // get centroids' values
        for(int i = 0; i < lines.length; i++)
            centroids.add(new Centroid(Integer.toString(i),lines[i]));
	}
	
	// for each record (datapoint) assign closest centroid
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		Centroid cluster = new Centroid();			// centroid to be assigned
		point.parse(value.toString());				// convert data value to Point object
		
		double minimumDistance = Double.MAX_VALUE;	// assign maximum value as default minimum distance
		
		for(int i=0; i<centroids.size(); i++) {									// for each centroid
			double distance = point.getDistance(centroids.get(i).getPoint());	// calculate distance from point to centroid
			if (distance < minimumDistance) {		// if distance is shorter than previous ones
                	cluster = centroids.get(i);		// assign new centroid to point
                	minimumDistance = distance;		// update minimumDistance with new distance
            }
		}
		
		context.write(cluster, point);			// write output record (key: cluster, value: point)
	}
	
}
