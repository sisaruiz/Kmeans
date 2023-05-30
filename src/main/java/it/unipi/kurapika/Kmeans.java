package it.unipi.kurapika;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import it.unipi.kurapika.utilities.*;


public class Kmeans {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
        conf.addResource(new Path("configuration.xml")); 

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: <input> <output>");
            System.exit(1);
        }

        // set parameters
        final String INPUT = otherArgs[0];
        final String OUTPUT = otherArgs[1] + "/temp";
        final int DATASET_SIZE = conf.getInt("dataset", 100);
        final String CENTROIDS_PATH = conf.get("centroids");
        final int K = conf.getInt("k", 3);
        final int MAX_ITERATIONS = conf.getInt("iterations", 20);

        Point[] newCentroids = new Point[K];

        // generate initial centroids
        newCentroids = Utility.generateCentroids(conf, INPUT, K, DATASET_SIZE);
        Utility.writeCentroids(conf, new Path(CENTROIDS_PATH), newCentroids);

        boolean stop = false;
        boolean succeded = true;
        int iteration = 0;
        while(!stop) {
            iteration++;
            
            // set job configuration
            Job job = Job.getInstance(conf, "iter_" + iteration);
            
            job.setJarByClass(Kmeans.class);
            job.setMapperClass(KmeansMapper.class);
            job.setCombinerClass(KmeansCombiner.class);
            job.setReducerClass(KmeansReducer.class);  
            
            job.setNumReduceTasks(K); 	           
            
            job.setOutputKeyClass(Centroid.class);
            job.setOutputValueClass(NullWritable.class);
            
            FileInputFormat.addInputPath(job, new Path(INPUT));
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT));
            
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            succeded = job.waitForCompletion(true);

            if(!succeded) {		// if a job fails exit program
                System.err.println("Job" + iteration + "failed.");
                System.exit(1);	
            }

            // check centroids' convergence
            stop = (0L == job.getCounters().findCounter(KmeansReducer.Counter.CONVERGED).getValue());

            // if centroids converged or the maximum number of iterations has been reached
            if(stop || iteration == (MAX_ITERATIONS -1)) {
            	// write final centroids in output file
                Utility.writeOutput(conf, new Path(CENTROIDS_PATH), new Path(otherArgs[1]));
                stop = true;	// stop iterations
            }
        }
        
        System.out.println("n_iter: " + iteration);

        System.exit(0);		
		
	}
	
}
