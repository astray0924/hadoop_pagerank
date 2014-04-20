package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper.JobGraphMapper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageMetaNodeWritable;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNodeWritable;

public class JobGraphTest {
	private static Map<Text, PageMetaNodeWritable> graph;
	static {
		graph = new HashMap<Text, PageMetaNodeWritable>();
		PageMetaNodeWritable A = new PageMetaNodeWritable(1, "A",
				Arrays.asList("B", "C", "E"), 3, 0.0f);
		// PageMetaNodeWritable B = new PageMetaNodeWritable(2, "B",
		// Arrays.asList("C"), 1, 0.0f);
		// PageMetaNodeWritable C = new PageMetaNodeWritable(3, "C",
		// new ArrayList<String>(), 0, 0.0f);
		// PageMetaNodeWritable D = new PageMetaNodeWritable(4, "D",
		// Arrays.asList("A"), 1, 0.0f);
		// PageMetaNodeWritable E = new PageMetaNodeWritable(5, "E",
		// Arrays.asList("D"), 1, 0.0f);
		graph.put(A.getTitle(), A);
	}

	private static Map<Text, VIntWritable> index;
	static {
		index = new HashMap<Text, VIntWritable>();
		index.put(new Text("A"), new VIntWritable(1));
		index.put(new Text("B"), new VIntWritable(2));
		index.put(new Text("C"), new VIntWritable(3));
		index.put(new Text("D"), new VIntWritable(4));
		index.put(new Text("E"), new VIntWritable(5));
	}

	@Before
	public void setUp() {

	}

	@Test
	public void testSingleNode() throws IOException {
		PageMetaNodeWritable A = graph.get("A");
		JobGraphMapper mapper = new JobGraphMapper();
		mapper.setIndex(index);

		MapDriver<Text, PageMetaNodeWritable, PageRankNodeWritable, PageRankNodeWritable> driver = new MapDriver<Text, PageMetaNodeWritable, PageRankNodeWritable, PageRankNodeWritable>();
		driver.withMapper(mapper).withInput(A.getTitle(), A);

		List<Pair<PageRankNodeWritable, PageRankNodeWritable>> outputs = driver
				.run();
		
		for (Pair<PageRankNodeWritable, PageRankNodeWritable> pair : outputs) {
			System.out.print(pair.getFirst());
			System.out.print("\t");
			System.out.print(pair.getFirst());
			System.out.print("\n");
		}
	}
}
