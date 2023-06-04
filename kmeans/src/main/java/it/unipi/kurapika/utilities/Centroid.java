package it.unipi.kurapika.utilities;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Centroid implements WritableComparable<Centroid>{
	
	private Text index = new Text();
	private Point point;
	
	public Centroid() {
		index = new Text();
        point = new Point();
	}
	
	public Centroid(Centroid center) {
		this.index = center.index;
        point = new Point(center.point);
	}
	
	public Centroid(String index, String values) {
		this();
		this.index.set(index);
		point.parse(values);		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		index.write(out);
        point.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		 index.readFields(in);
	     point.readFields(in);
	}

	@Override
	public int compareTo(Centroid o) {
		// TODO Auto-generated method stub
		return this.index.compareTo(o.index);
	}
	
	public Text getLabel(){
		return this.index;
	}
	
	public Point getPoint() {
		return this.point;
	}
	
	public void setIndex(Centroid cen) {
		this.index.set(cen.index);
	}

}
