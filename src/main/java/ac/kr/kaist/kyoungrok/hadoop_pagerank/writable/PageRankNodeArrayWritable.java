package ac.kr.kaist.kyoungrok.hadoop_pagerank.writable;


import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class PageRankNodeArrayWritable extends ArrayWritable {
	public PageRankNodeArrayWritable() {
		super(PageRankNodeArrayWritable.class);
	}
	
	public PageRankNodeArrayWritable(Writable[] values) {
		super(PageRankNodeArrayWritable.class, values);
	}

	public int getSize() {
		return get().length;
	}
	
	public String toString() {
		return getSize() + " elements";
	}
}
