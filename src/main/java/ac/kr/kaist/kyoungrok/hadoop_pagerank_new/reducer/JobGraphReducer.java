package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.reducer;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNodeArrayWritable;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNode;

public class JobGraphReducer
		extends
		Reducer<PageRankNode, PageRankNode, PageRankNode, PageRankNodeArrayWritable> {

	@Override
	public void reduce(PageRankNode node,
			Iterable<PageRankNode> inNodes, Context context) {
		List<PageRankNode> ins = new ArrayList<PageRankNode>();
		for (PageRankNode inNode : inNodes) {
			ins.add(inNode);
		}
		
		
	}
}
