package it.unipi.kurapika;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import it.unipi.kurapika.utilities.*;


public class Kmeans {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 5) {
            System.err.println("Usage: <input> <output> <n_clusters> <dataset_size> <max_n_iter>");
            System.exit(1);
        }

        // set custom parameters
        final Path inputPath = new Path(otherArgs[0]);
        final Path outputPath = new Path(otherArgs[1]);
        final int k = Integer.parseInt(otherArgs[2]);
        final int datasetSize = Integer.parseInt(otherArgs[3]);
        final int maxIter = Integer.parseInt(otherArgs[4]);
	
        // set default parameters
        conf.setDouble("epsilon", 1.0);

        // generate initial centroids
        Utility.generateCentroids(conf, inputPath, k, datasetSize);

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
            
            job.setNumReduceTasks(k); 	           
            
            job.setMapOutputKeyClass(Centroid.class);
            job.setMapOutputValueClass(Point.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            
            FileInputFormat.addInputPath(job, inputPath);
            outputPath.getFileSystem(conf).delete(outputPath, true);
            FileOutputFormat.setOutputPath(job, outputPath);
            
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            succeded = job.waitForCompletion(true);

            if(!succeded) {		// if a job fails exit program
                System.err.println("Job" + iteration + "failed.");
                System.exit(2);	
            }

            // set new centroids
            Utility.setNewCentroids(conf, outputPath, k);
            
            // check centroids' convergence
            stop = (0L == job.getCounters().findCounter(KmeansReducer.Counter.CONVERGED).getValue());

            // if centroids converged or the maximum number of iterations has been reached
            if(stop || iteration == (maxIter) ) {
                stop = true;	// stop iterations
            }
        }
        
        System.out.println("n_iter: " + (iteration));

        System.exit(0);		
		
	}
	
}
