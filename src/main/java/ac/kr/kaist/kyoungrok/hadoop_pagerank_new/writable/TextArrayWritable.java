package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable;


import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class TextArrayWritable extends ArrayWritable {
	public TextArrayWritable() {
		super(Text.class);
	}

	public int getSize() {
		return get().length;
	}
	
	public String toString() {
		return getSize() + " elements";
	}
}
