package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper;

import java.io.IOException;
import java.net.URI;
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

public class MapperOutDegree extends
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
		
		Object[] out = index.values().toArray();

	}

	@SuppressWarnings("deprecation")
	private void readIndexFromCache(Context context) throws IOException {
		Configuration conf = context.getConfiguration();

		URI[] files = context.getCacheFiles();

		FileSystem fs = PathHelper.getFileSystem(new Path(files[0]),
				context.getConfiguration());
		Text title = new Text("");
		VIntWritable id = new VIntWritable(0);
		for (URI path : files) {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
					path), conf);

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
			
//			System.out.printf("%s - %s\n", linkTitle, index.get(linkTitle));

			if (index.containsKey(linkTitle)) {
				VIntWritable linkId = index.get(linkTitle);
				context.write(id, linkId);
			}
		}
	}
}