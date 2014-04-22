package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util.PageRankHelper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNode;

public class MapperRank extends
		Mapper<VIntWritable, PageRankNode, VIntWritable, PageRankNode> {
	private Map<VIntWritable, PageRankNode> graph;
	private PageRankHelper helper;

	@Override
	protected void setup(Context context) throws IOException {
		if (graph == null) {
			graph = new HashMap<VIntWritable, PageRankNode>();
			buildIndexFromCache(context);
		}

		helper = new PageRankHelper(graph);
	}

	private void buildIndexFromCache(Context context) throws IOException {
		URI[] cacheFiles = context.getCacheFiles();

		if (cacheFiles == null || cacheFiles.length == 0) {
			throw new FileNotFoundException("graph file not found.");
		}

		// read index
		VIntWritable id = new VIntWritable();
		PageRankNode node = new PageRankNode();
		for (URI uri : cacheFiles) {
			MapFile.Reader reader = new MapFile.Reader(new Path(uri),
					context.getConfiguration());

			try {

				while (reader.next(id, node)) {
					graph.put(new VIntWritable(id.get()),
							new PageRankNode(node));
				}
			} finally {
				reader.close();
			}
		}
	}

	@Override
	public void map(VIntWritable id, PageRankNode node, Context context)
			throws IOException, InterruptedException {
		float newScore = helper.getNewScore(node);
		node.updateScore(newScore);
		context.write(id, node);
	}

}
