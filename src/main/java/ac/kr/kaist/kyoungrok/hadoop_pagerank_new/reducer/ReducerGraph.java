package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNode;

public class ReducerGraph extends
		Reducer<VIntWritable, PageRankNode, VIntWritable, PageRankNode> {

	@Override
	public void reduce(VIntWritable id, Iterable<PageRankNode> nodes,
			Context context) throws IOException, InterruptedException {
		PageRankNode node = nodes.iterator().next();
		context.write(id, node);
	}
}
