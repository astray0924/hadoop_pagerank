package ac.kr.kaist.kyoungrok.hadoop_pagerank.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import ac.kr.kaist.kyoungrok.hadoop_pagerank.util.PathHelper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.writable.PageMetaNode;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.writable.TextArrayWritable;

public class MapperInDegree extends
		Mapper<Text, PageMetaNode, VIntWritable, VIntWritable> {
	public enum InDegreeCounter {
		HIT, MISSED, INDEX_SIZE
	}

	private Map<Text, VIntWritable> index;

	@Override
	protected void setup(Context context) throws IOException {
		if (index == null) {
			index = new HashMap<Text, VIntWritable>();
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
				while (reader.next(title, id)) {
					index.put(new Text(title), new VIntWritable(id.get()));
					context.getCounter(InDegreeCounter.INDEX_SIZE).increment(1);
				}
			} finally {
				reader.close();
			}

		}
	}

	public void setIndex(Map<Text, VIntWritable> index) {
		this.index = index;
	}

	@Override
	public void map(Text title, PageMetaNode node, Context context)
			throws IOException, InterruptedException {
		VIntWritable id = node.getId();

		TextArrayWritable linkTitles = node.getOutLinks();
		Text linkTitle = new Text();
		boolean found = false;
		for (Writable lt : linkTitles.get()) {
			linkTitle = (Text) lt;

			if (index.containsKey(linkTitle)) {
				VIntWritable linkId = index.get(linkTitle);
				context.write(linkId, id);
				found = true;
			}
		}

		if (found) {
			context.getCounter(InDegreeCounter.HIT).increment(1);
		} else {
			context.getCounter(InDegreeCounter.MISSED).increment(1);
		}
	}
}
