package it.unipi.kurapika;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.unipi.kurapika.utilities.*;

public class KmeansMapper extends Mapper<LongWritable, Text, Centroid, Point>{
	
	private Point point;
	private List<Centroid> centroids = new ArrayList<>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		Path centroidsPath = new Path(conf.get("centroids"));

		try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centroidsPath))) {
			
			Centroid key = new Centroid();
			
			while (reader.next(key)) {
				Centroid center = new Centroid(key);
				centroids.add(center);
			}
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		Centroid cluster = null;
		point.parse(value.toString());
		
		double minimumDistance = Double.MAX_VALUE;
		
		for(int i=0; i<centroids.size(); i++) {
			double distance = point.getDistance(centroids.get(i).getPoint());
			if (distance < minimumDistance) {
                cluster = centroids.get(i);
                minimumDistance = distance;
            }
		}
		
		context.write(cluster, point);
	}
	
}
