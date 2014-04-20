package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper.JobGraphMapper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageMetaNode;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNode;

public class TestJobGraph {
	private static Map<Text, PageMetaNode> graph;
	private static Map<Text, VIntWritable> index;

	@Before
	public void setUp() {
		// graph
		graph = new HashMap<Text, PageMetaNode>();
		PageMetaNode A = new PageMetaNode(1, "A", Arrays.asList("B", "C", "E"),
				3, 0.0f);
		PageMetaNode B = new PageMetaNode(2, "B", Arrays.asList("C"), 1, 0.0f);
		PageMetaNode C = new PageMetaNode(3, "C", new ArrayList<String>(), 0,
				0.0f);
		PageMetaNode D = new PageMetaNode(4, "D", Arrays.asList("A"), 1, 0.0f);
		PageMetaNode E = new PageMetaNode(5, "E", Arrays.asList("D"), 1, 0.0f);
		graph.put(A.getTitle(), A);
		graph.put(B.getTitle(), B);
		graph.put(C.getTitle(), C);
		graph.put(D.getTitle(), D);
		graph.put(E.getTitle(), E);

		// Index
		index = new HashMap<Text, VIntWritable>();
		index.put(new Text("A"), A.getId());
		index.put(new Text("B"), B.getId());
		index.put(new Text("C"), C.getId());
		index.put(new Text("D"), D.getId());
		index.put(new Text("E"), E.getId());

		// debugGraph();
	}

	public void debugGraph() {
		for (PageMetaNode node : graph.values()) {
			System.out.println(node);
		}
	}

	@Test
	public void testAllNodes() throws IOException {
		JobGraphMapper mapper = new JobGraphMapper();
		mapper.setIndex(index);
		MapDriver<Text, PageMetaNode, PageRankNode, PageRankNode> driver = new MapDriver<Text, PageMetaNode, PageRankNode, PageRankNode>();

		for (PageMetaNode node : graph.values()) {
			testSingleNode(node);
		}
	}

	public void testSingleNode(PageMetaNode node) throws IOException {
		JobGraphMapper mapper = new JobGraphMapper();
		mapper.setIndex(index);

		MapDriver<Text, PageMetaNode, VIntWritable, PageRankNode> driver = new MapDriver<Text, PageMetaNode, VIntWritable, PageRankNode>();
		driver.withMapper(mapper).withInput(node.getTitle(), node);

		List<Pair<VIntWritable, PageRankNode>> outputs = driver.run();

		for (Pair<VIntWritable, PageRankNode> pair : outputs) {
			System.out.print(pair.getFirst());
			System.out.print("\t");
			System.out.print(pair.getSecond());
			System.out.print("\n");
		}
	}
}
