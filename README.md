# K-means algorithm implementation with MapReduce
## Updates (to be committed) for better efficiency:
- don't use WriteComparableObject Centroid (Point is enough)
- fields inside the custom class can be primitive types (not necessarily writable)
- in emitting use simple writable types not entire writable object (lighter)
- increment converged counter in driver not in reducer
