package org.apache.nutch.comparison;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class CompositeWritable implements WritableComparable<CompositeWritable> {
	private Text text;
	private DoubleWritable hrScore;
	private DoubleWritable trScore;
	private DoubleWritable urScore;
	
	public CompositeWritable(){
		set(new Text(), new DoubleWritable(), new DoubleWritable(), new DoubleWritable());
	}
	
	public CompositeWritable(Text text, DoubleWritable hrScore, DoubleWritable trScore, DoubleWritable urScore){
		set(text, hrScore, trScore, urScore);
	}

	public void set(Text text, DoubleWritable hrScore, DoubleWritable trScore, DoubleWritable urScore){
		this.text = text;
		this.hrScore = hrScore;
		this.trScore = trScore;
		this.urScore = urScore;
	}
	
	public Text getText(){
		return text;
	}
	
	public DoubleWritable getHrScore(){
		return hrScore;
	}
	
	public DoubleWritable getTotal(){
		return new DoubleWritable(hrScore.get() + urScore.get() + trScore.get());
	}
	
	public DoubleWritable getTrScore(){
		return trScore;
	}
	
	public DoubleWritable getUrScore(){
		return urScore;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		text.readFields(in);
		hrScore.readFields(in);
		trScore.readFields(in);
		urScore.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		text.write(out);
		hrScore.write(out);
		trScore.write(out);
		urScore.write(out);
	}

	@Override
	public int compareTo(CompositeWritable o) {
		return (o.getTotal()).compareTo(getTotal());
	}
	
	@Override
	public String toString(){
		return text + "\t" + hrScore + "\t" + urScore + "\t" + trScore;
	}

}
