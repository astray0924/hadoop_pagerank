package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageMetaNode;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNode;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.VIntArrayWritable;

public class JobGraphMapper extends
		Mapper<Text, PageMetaNode, VIntWritable, PageRankNode> {
	private Map<Text, VIntWritable> index;
	private FloatWritable initialScore;

	@Override
	protected void setup(Context context) throws IOException {
		if (index == null) {
			buildIndexFromCache(context);
		}

		// set the initial score
		this.initialScore = new FloatWritable((float) 1 / index.size());
	}

	private void buildIndexFromCache(Context context) throws IOException {
		HashMap<Text, VIntWritable> index = new HashMap<Text, VIntWritable>();
		URI[] cacheFiles = context.getCacheFiles();

		if (cacheFiles == null || cacheFiles.length == 0) {
			throw new FileNotFoundException("title-id index file not found.");
		}

		// read index
		FileSystem fs = FileSystem.get(cacheFiles[0],
				context.getConfiguration());
		for (URI uri : cacheFiles) {
			@SuppressWarnings("deprecation")
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
					uri), context.getConfiguration());

			try {
				Text title = new Text();
				VIntWritable id = new VIntWritable();

				while (reader.next(title, id)) {
					index.put(title, id);
				}
			} finally {
				reader.close();
			}
		}

		setIndex(index);
	}

	public void setIndex(Map<Text, VIntWritable> index) {
		this.index = index;
	}

	@Override
	protected void cleanup(Context context) {

	}

	@Override
	public void map(Text nodeTitle, PageMetaNode metaNode, Context context)
			throws IOException, InterruptedException {
		PageRankNode node = new PageRankNode();
		for (Writable l : metaNode.getOutLinks().get()) {
			Text outTitle = (Text) l;
			if (index.containsKey(outTitle)) {
				VIntWritable outId = index.get(outTitle);
				VIntArrayWritable inLinks = new VIntArrayWritable();
				inLinks.set(new VIntWritable[]{metaNode.getId()});
				
				node.set(outId, new VIntWritable(-1), inLinks, initialScore);					
			}
			
		}

	}
}
