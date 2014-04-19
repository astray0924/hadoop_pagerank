package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageMetaNodeWritable;

public class JobParseReducer extends
		Reducer<Text, PageMetaNodeWritable, Text, PageMetaNodeWritable> {
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
	public void reduce(Text title, Iterable<PageMetaNodeWritable> nodes,
			Context context) throws IOException, InterruptedException {
		PageMetaNodeWritable node = nodes.iterator().next();

		mos.write("metanodes", title, node, "metanodes/metanodes");
		mos.write("titleidmap", title, node.getId(), "titleidmap/titleidmap");
		mos.write("idtitlemap", node.getId(), title, "idtitlemap/idtitlemap");
	}
}
