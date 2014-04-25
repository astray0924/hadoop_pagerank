package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankResult;

public class ReducerResult extends
		Reducer<PageRankResult, NullWritable, PageRankResult, NullWritable> {
	@Override
	public void reduce(PageRankResult result, Iterable<NullWritable> temp,
			Context context) throws IOException, InterruptedException {
		context.write(result, NullWritable.get());
	}
}
