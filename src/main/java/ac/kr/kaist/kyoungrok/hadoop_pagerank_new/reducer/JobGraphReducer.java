package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.reducer;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNodeArrayWritable;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNodeWritable;

public class JobGraphReducer
		extends
		Reducer<PageRankNodeWritable, PageRankNodeWritable, PageRankNodeWritable, PageRankNodeArrayWritable> {

	@Override
	public void reduce(PageRankNodeWritable node,
			Iterable<PageRankNodeWritable> inNodes, Context context) {
		List<PageRankNodeWritable> ins = new ArrayList<PageRankNodeWritable>();
		for (PageRankNodeWritable inNode : inNodes) {
			ins.add(inNode);
		}
		
		
	}
}
