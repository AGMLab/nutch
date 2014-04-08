package org.apache.nutch.comparison;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
 
public class ActualKeyGroupingComparator extends WritableComparator {
 
	protected ActualKeyGroupingComparator() {
	 
		super(DoubleWritable.class, true);
	}
	 
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		return 0;
	}
}