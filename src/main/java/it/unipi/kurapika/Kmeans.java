package it.unipi.kurapika;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import it.unipi.kurapika.utilities.*;


public class Kmeans {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		int iteration = 1;
		final String SEP = System.getProperty("file.separator");
		Configuration conf = new Configuration(); 
		conf.addResource(new Path("config.xml"));
		conf.set("num.iteration", iteration + "");
		
		final int DATASET_SIZE = conf.getInt("dataset", 10);
        final int DISTANCE = conf.getInt("distance", 2);
        final int K = conf.getInt("k", 3);
		
		Path in = new Path("files/clustering/import/data");
		Path center = new Path("files/clustering/import/center/cen.seq");
		conf.set("centroids", center.toString());
		Path out =  new Path(conf.get("output") + SEP + String.valueOf(conf.getInt("iteration", 0)));
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}

		if (fs.exists(center)) {
			fs.delete(out, true);
		}

		if (fs.exists(in)) {
			fs.delete(in, true);
		}
		
		Job job = Job.getInstance(conf);
		
		FileOutputFormat.setOutputPath(job, in);
        FileInputFormat.addInputPath(job, out);
        
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(KmeansMapper.class);
		job.setCombinerClass(KmeansCombiner.class);
		job.setReducerClass(KmeansReducer.class);
		
		Point[] newCentroids = null;
		newCentroids = Utility.centroidsInit(conf, in.toString(), K, DATASET_SIZE);
		Utility.writeExampleCenters(conf, center, newCentroids);
		
		
		job.setMapOutputKeyClass(Centroid.class);
	    job.setMapOutputValueClass(Point.class);
	    job.setOutputKeyClass(Centroid.class);
	    job.setOutputValueClass(NullWritable.class);		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.waitForCompletion(true);
		
		long counter = job.getCounters().findCounter(KmeansReducer.Counter.CONVERGED).getValue();
		iteration++;
		
		while (counter > 0) {
			conf = new Configuration();
			conf.set("centroid.path", center.toString());
			conf.set("num.iteration", iteration + "");
			job = Job.getInstance(conf);
			job.setJobName("KMeans Clustering " + iteration);

			job.setMapperClass(KmeansMapper.class);
			job.setReducerClass(KmeansReducer.class);
			job.setJarByClass(KmeansMapper.class);

			in = new Path("files/clustering/depth_" + (iteration - 1) + "/");
			out = new Path("files/clustering/depth_" + iteration);

			FileInputFormat.addInputPath(job, in);
			if (fs.exists(out))
				fs.delete(out, true);

			FileOutputFormat.setOutputPath(job, out);
			job.setMapOutputKeyClass(Centroid.class);
		    job.setMapOutputValueClass(Point.class);
		    job.setOutputKeyClass(Centroid.class);
		    job.setOutputValueClass(NullWritable.class);		
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			job.waitForCompletion(true);
			iteration++;
			counter = job.getCounters().findCounter(KmeansReducer.Counter.CONVERGED).getValue();
		}

	}
	
}
