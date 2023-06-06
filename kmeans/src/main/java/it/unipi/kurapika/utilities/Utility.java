package it.unipi.kurapika.utilities;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class Utility {
	
	final static String FILE_NAME = "part-r-000";

	// generate random k centroids
	public static void generateCentroids(Configuration conf, Path inPath, int k, int dataSetSize) throws IOException {
		
		// get random indexes
		List<Integer> positions = new ArrayList<Integer>();
		Random random = new Random();
		int pos;
		
		while(positions.size() < k) {
			pos = random.nextInt(dataSetSize);
		    if(!positions.contains(pos)) {
		    	positions.add(pos);
		    }
		}
		Collections.sort(positions);
		  
		// get elements
		FileSystem hdfs = FileSystem.get(conf);
		FSDataInputStream in = hdfs.open(inPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		int line = 0;
		int i = 0;
		int position;
		String[] centroids = new String[k];
		while(i < positions.size()) {
			position = positions.get(i);
		    String point = br.readLine();
		    if(line == position) {    
		    	centroids[i] = point;
		        i++;
		    }
		    line++;
		}
		br.close();
		// save centroids
		conf.setStrings("centroids", centroids);
	}
	
	public static void setNewCentroids(Configuration conf, Path outPath, int clusters) {
		
		BufferedReader br = null;
        FileSystem fs = null;
        String [] centroids = new String[clusters];
        int index = 0;
        
        // Merging output files from multiple reducers
        for(int fileNumber = 0; fileNumber < clusters; fileNumber++ ){
            String name = "";
            if(fileNumber < 10)
                name = "0" + fileNumber;
            else 
                name = String.valueOf(fileNumber);
            String path = outPath + "/" + FILE_NAME + name;
            Path pt = new Path(path);
            try {
                // The lines of the output files are inspected and the shift for the new centroids is computed
                fs = outPath.getFileSystem(conf);
                br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                String temp = "";
                while((line = br.readLine()) != null){
                    String[] split = line.split("\\s+");
                    int j=1;
                    int i = j+conf.getInt("dim", 2);
                    while(j<split.length) {
                    	temp += split[j] + " ";
                    	if (j == i) {
                    		centroids[index] = temp;
                    		temp ="";
                    		i+=conf.getInt("dim", 2);
                    		j++;
                    		index++;
                    	}
                    	j++;
                    }
                }
                br.close();
                fs.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } 
        }

        // add to conf
        conf.setStrings("centroids", centroids);
	}
}
