package ac.kr.kaist.kyoungrok.hadoop_pagerank.writable;


import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

public class LongArrayWritable extends ArrayWritable {
	public LongArrayWritable() {
		super(LongWritable.class);
	}

	public int getSize() {
		return get().length;
	}
	
	public String toString() {
		return getSize() + " elements";
	}
}
