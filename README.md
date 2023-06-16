# K-means algorithm implementation with MapReduce

## How to run
After installing Hadoop in fully-distributed mode and running, execute with command:
```bash
hadoop jar kmeans-1.0-SNAPSHOT.jar it.unipi.kurapika.Kmeans points.txt output 2 4 100 8
```
(```bashpoints.txt``` must be already present in HDFS file system)

### Updates (to be committed) for better efficiency:
- don't use WriteComparableObject Centroid (Point is enough)
- fields inside the custom class can be primitive types (not necessarily writable)
- in emitting use simple writable types not entire writable object (lighter)
- increment converged counter in driver not in reducer
