package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable;


import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class VIntArrayWritable extends ArrayWritable {
	public VIntArrayWritable() {
		super(VIntArrayWritable.class);
	}
	
	public VIntArrayWritable(Writable[] values) {
		super(VIntArrayWritable.class, values);
	}

	public int getSize() {
		return get().length;
	}
	
	public String toString() {
		return getSize() + " elements";
	}
}
