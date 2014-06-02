package ac.kr.kaist.kyoungrok.hadoop_pagerank.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import ac.kr.kaist.kyoungrok.hadoop_pagerank.util.PathHelper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.writable.PageMetaNode;

public class ReducerParse extends
		Reducer<Text, PageMetaNode, Text, PageMetaNode> {
	private enum Page {
		WIKI_PAGE_PARSED
	}

	@SuppressWarnings("rawtypes")
	private MultipleOutputs mos;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void setup(Context context) {
		mos = new MultipleOutputs(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

	@SuppressWarnings("unchecked")
	public void reduce(Text title, Iterable<PageMetaNode> nodes, Context context)
			throws IOException, InterruptedException {
		PageMetaNode node = nodes.iterator().next();

		mos.write(PathHelper.NAME_META_NODES, title, node, String
				.format("%s/%s", PathHelper.NAME_META_NODES,
						PathHelper.NAME_META_NODES));
		mos.write(PathHelper.NAME_TITLE_ID_MAP, title, node.getId(), String
				.format("%s/%s", PathHelper.NAME_TITLE_ID_MAP,
						PathHelper.NAME_TITLE_ID_MAP));
		mos.write(PathHelper.NAME_ID_TITLE_MAP, node.getId(), title, String
				.format("%s/%s", PathHelper.NAME_ID_TITLE_MAP,
						PathHelper.NAME_ID_TITLE_MAP));

		context.getCounter(Page.WIKI_PAGE_PARSED).increment(1);
	}
}
