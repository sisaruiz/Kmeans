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
	private ArrayPrimitiveWritable coordinates = new ArrayPrimitiveWritable();
	private IntWritable numPoints = new IntWritable();
	
	public Point() {
		this.dimension = new IntWritable(0);
		double[] vector = new double[this.dimension.get()];
		for (int i=0; i<this.dimension.get(); i++) {
			vector[i] = 0.0;
		}
		this.coordinates.set(vector);
		this.numPoints.set(0);
	}
	
	public Point(Point point) {
		this.dimension = point.dimension;
		double[] vector = (double[])point.coordinates.get();
        this.coordinates.set(Arrays.copyOf(vector, vector.length));
        this.numPoints.set(point.numPoints.get());
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		dimension.write(out);
		coordinates.write(out);
		numPoints.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		dimension.readFields(in);
		coordinates.readFields(in);
		numPoints.readFields(in);
	}
	
	public void parse(String values) {
        String[] coords = values.split(" ");
        double[] tmp = new double[coords.length];
        for (int i = 0; i < tmp.length; i++) {
            tmp[i] = Double.valueOf(coords[i]);
        }

        coordinates.set(tmp);
        dimension.set(coords.length);
        numPoints.set(1);
    }
	
	public double getDistance(Point centroid) {
		double sum = 0;
		double [] thisCoord = (double[])this.coordinates.get();
		double [] centrCoord = (double[])centroid.coordinates.get();
		
        for (int i = 0; i < this.dimension.get(); i++) {
            sum += Math.pow(Math.abs(thisCoord[i] - centrCoord[i]), 2);
        }
        return Math.sqrt(sum);
	}
	
	public void sum(Point p) {
		double [] thisCoord = (double[])this.coordinates.get();
		double [] otherCoord = (double[])p.coordinates.get();
		
		for (int i = 0; i < this.dimension.get(); i++) {
            thisCoord[i] += otherCoord[i];
        }
		
		this.coordinates.set(thisCoord);
		
        this.numPoints.set(this.numPoints.get()+p.numPoints.get());
	}
	
	public void compress() {
		double [] toUpdateCoord = (double[])this.coordinates.get();
		
		for(int i=0; i<this.dimension.get(); i++) {
			toUpdateCoord[i] = toUpdateCoord[i]/this.numPoints.get();
		}
		this.coordinates.set(toUpdateCoord);
		this.numPoints.set(1);
	}
	
	@Override
    public String toString(){
        String temp = "";
        double [] thisCoord = (double[])this.coordinates.get();
        
        for (int i = 0; i < dimension.get() ; i++)
            temp += thisCoord[i] + " ";
        return temp;
    }
}





