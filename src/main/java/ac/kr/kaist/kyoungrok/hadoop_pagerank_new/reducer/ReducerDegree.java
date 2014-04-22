package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.VIntArrayWritable;

public class ReducerDegree extends
		Reducer<VIntWritable, VIntWritable, VIntWritable, VIntArrayWritable> {
	@Override
	public void reduce(VIntWritable id, Iterable<VIntWritable> linksIter,
			Context context) throws IOException, InterruptedException {
		List<VIntWritable> links = new ArrayList<VIntWritable>();
		for (VIntWritable l : linksIter) {
			links.add(new VIntWritable(l.get()));
		}

		VIntArrayWritable linksWritable = new VIntArrayWritable(links);

		context.write(id, linksWritable);
	}
}
