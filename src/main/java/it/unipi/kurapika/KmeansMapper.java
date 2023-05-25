package it.unipi.kurapika;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.unipi.kurapika.utilities.*;

public class KmeansMapper extends Mapper<Object, Text, Centroid, Point>{
	
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
	
	
}
