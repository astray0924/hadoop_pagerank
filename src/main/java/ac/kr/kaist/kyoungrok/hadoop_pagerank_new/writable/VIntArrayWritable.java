package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable;

import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Joiner;

public class VIntArrayWritable extends ArrayWritable {
	public VIntArrayWritable() {
		super(VIntWritable.class);
		set(new VIntWritable[] {});
	}

	public VIntArrayWritable(Writable[] values) {
		super(VIntWritable.class, values);
	}

	public VIntArrayWritable(List<VIntWritable> values) {
		super(VIntWritable.class, values
				.toArray(new VIntWritable[values.size()]));
	}

	public int getSize() {
		return get().length;
	}

	public String toString() {
		return Joiner.on(",").join(get());
	}

}
