package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageMetaNodeWritable;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNodeWritable;

public class JobGraphMapper extends
		Mapper<Text, PageMetaNodeWritable, PageRankNodeWritable, NullWritable> {
	@Override
	protected void setup(Context context) {

	}

	@Override
	protected void cleanup(Context context) {

	}

	@Override
	public void map(Text title, PageMetaNodeWritable metaNode, Context context) {

	}

}
