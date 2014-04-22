package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper;

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

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util.PathHelper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageMetaNode;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.TextArrayWritable;

public class MapperInDegree extends
		Mapper<Text, PageMetaNode, VIntWritable, VIntWritable> {
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

		Path[] files = PathHelper.getCacheFiles(PathHelper.NAME_TITLE_ID_MAP,
				conf);

		Text title = new Text("");
		VIntWritable id = new VIntWritable(0);
		FileSystem fs = PathHelper.getFileSystem(files[0],
				context.getConfiguration());
		for (Path path : files) {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

			try {
				while (reader.next(title, id)) {
					index.put(new Text(title), new VIntWritable(id.get()));
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
		for (Writable lt : linkTitles.get()) {
			linkTitle = (Text) lt;

			if (index.containsKey(linkTitle)) {
				VIntWritable linkId = index.get(linkTitle);
				context.write(linkId, id);
			}
		}
	}
}
