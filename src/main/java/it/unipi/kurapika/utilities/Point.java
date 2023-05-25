package it.unipi.kurapika.utilities;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Arrays;

public class Point implements Writable {
	
	private IntWritable dimension;
	private ArrayPrimitiveWritable coordinates = null;
	
	public Point() {
		this.dimension = new IntWritable(0);
		this.coordinates = new ArrayPrimitiveWritable();
	}
	
	public Point(Point point) {
		this.dimension = point.dimension;
		double[] vector = (double[])point.coordinates.get();
        this.coordinates.set(Arrays.copyOf(vector, vector.length));
	}
	
	public Point(double[] seq, int dim) {
		this();
		this.coordinates.set(seq);
		this.dimension.set(dim);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		dimension.write(out);
		coordinates.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		dimension.readFields(in);
		coordinates.readFields(in);
	}
	
}