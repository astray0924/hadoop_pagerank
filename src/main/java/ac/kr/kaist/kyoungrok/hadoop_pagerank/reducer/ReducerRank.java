package ac.kr.kaist.kyoungrok.hadoop_pagerank.reducer;

import java.io.IOException;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import ac.kr.kaist.kyoungrok.hadoop_pagerank.writable.PageRankNode;

public class ReducerRank extends
		Reducer<VIntWritable, PageRankNode, VIntWritable, PageRankNode> {
	@Override
	public void reduce(VIntWritable id, Iterable<PageRankNode> nodes,
			Context context) throws IOException, InterruptedException {
		context.write(id, nodes.iterator().next());
	}

}
