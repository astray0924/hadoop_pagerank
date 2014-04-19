package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable;


import org.apache.hadoop.io.ArrayWritable;

public class VIntArrayWritable extends ArrayWritable {
	public VIntArrayWritable() {
		super(VIntArrayWritable.class);
	}

	public int getSize() {
		return get().length;
	}
	
	public String toString() {
		return getSize() + " elements";
	}
}
