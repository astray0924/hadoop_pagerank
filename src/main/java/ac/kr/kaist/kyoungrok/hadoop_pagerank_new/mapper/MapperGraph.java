package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNode;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.VIntArrayWritable;

public class MapperGraph extends
		Mapper<VIntWritable, VIntArrayWritable, VIntWritable, PageRankNode> {
	private Map<VIntWritable, VIntArrayWritable> outlinkIndex;
	private FloatWritable initialScore;

	@Override
	protected void setup(Context context) throws IOException {
		if (outlinkIndex == null) {
			outlinkIndex = new HashMap<VIntWritable, VIntArrayWritable>();
			buildIndexFromCache(context);
		}

		// set the initial score
		this.initialScore = new FloatWritable((float) 1 / outlinkIndex.size());
	}

	private void buildIndexFromCache(Context context) throws IOException {
		URI[] cacheFiles = context.getCacheFiles();

		if (cacheFiles == null || cacheFiles.length == 0) {
			throw new FileNotFoundException("outlink index file not found.");
		}

		// read index
		VIntWritable id = new VIntWritable();
		VIntArrayWritable inLinks = new VIntArrayWritable();
		for (URI uri : cacheFiles) {
			MapFile.Reader reader = new MapFile.Reader(new Path(uri),
					context.getConfiguration());

			try {

				while (reader.next(id, inLinks)) {
					outlinkIndex.put(new VIntWritable(id.get()), new VIntArrayWritable(inLinks.get()));
				}
			} finally {
				reader.close();
			}
		}
	}

	public void setIndex(Map<VIntWritable, VIntArrayWritable> outlinkIndex) {
		this.outlinkIndex = outlinkIndex;
	}

	@Override
	protected void cleanup(Context context) {

	}

	@Override
	public void map(VIntWritable id, VIntArrayWritable inLinks, Context context)
			throws IOException, InterruptedException {
		PageRankNode node = new PageRankNode();
		VIntWritable outCount = new VIntWritable(0);

		if (outlinkIndex.containsKey(id)) {
			outCount.set(outlinkIndex.get(id).getSize());
			node.set(id, outCount, inLinks, initialScore);
			context.write(id, node);
		}
	}
}
