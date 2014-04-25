package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util.PathHelper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNode;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankResult;

public class MapperResult extends
		Mapper<VIntWritable, PageRankNode, PageRankResult, NullWritable> {

	private Map<VIntWritable, Text> index;

	@Override
	public void setup(Context context) throws IOException {
		if (index == null) {
			index = new HashMap<VIntWritable, Text>();
			readIndexFromCache(context);
		}

		if (index == null) {
			throw new IllegalStateException("title-id index is missing!");
		}
	}

	private void readIndexFromCache(Context context) throws IOException {
		Configuration conf = context.getConfiguration();

		Path[] files = PathHelper.uris2Paths(context.getCacheFiles());

		Text title = new Text("");
		VIntWritable id = new VIntWritable(0);
		FileSystem fs = PathHelper.getFileSystem(files[0],
				context.getConfiguration());
		for (Path path : files) {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

			try {
				while (reader.next(id, title)) {
					index.put(new VIntWritable(id.get()), new Text(title));
				}
			} finally {
				reader.close();
			}

		}
	}

	@Override
	public void map(VIntWritable id, PageRankNode node, Context context)
			throws IOException, InterruptedException {
		if (index.containsKey(id)) {
			PageRankResult result = new PageRankResult();
			result.set(index.get(id), id, node.getScore());
			context.write(result, NullWritable.get());
		}

	}
}
