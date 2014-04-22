package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util.WikiDumpParser;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageMetaNode;

public class MapperParse extends
		Mapper<LongWritable, Text, Text, PageMetaNode> {
	private WikiDumpParser parser;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		parser = new WikiDumpParser();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String page = value.toString();
		parser.parse(page);

		// ns의 값이 0일때만 페이지 정보 추출 및 저장
		if (parser.getNs() == 0) {
			// 필요한 정보 추출
			String title = parser.getTitle();
			int id = parser.getId();
			List<String> outLinks = parser.getSanitizedLinks();

			PageMetaNode node = new PageMetaNode(id, title,
					outLinks, outLinks.size(), 0.0f);
			context.write(node.getTitle(), node);
		}
	}

}
