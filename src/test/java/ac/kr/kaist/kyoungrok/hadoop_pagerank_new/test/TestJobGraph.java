package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import ac.kr.kaist.kyoungrok.hadoop_pagerank.mapper.MapperGraph;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.writable.PageRankNode;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.writable.VIntArrayWritable;

public class TestJobGraph {
	private static Map<VIntWritable, PageRankNode> graph;
	private static Map<VIntWritable, VIntArrayWritable> input;
	private static Map<VIntWritable, VIntArrayWritable> index;
	private MapDriver<VIntWritable, VIntArrayWritable, VIntWritable, PageRankNode> mapDriver;

	@Before
	public void setUp() {
		// input
		input = new HashMap<VIntWritable, VIntArrayWritable>();

		VIntArrayWritable inLinks1 = new VIntArrayWritable();
		inLinks1.set(new VIntWritable[] { new VIntWritable(2),
				new VIntWritable(4) });
		input.put(new VIntWritable(1), inLinks1); // 1 - 2, 4

		VIntArrayWritable inLinks2 = new VIntArrayWritable();
		inLinks2.set(new VIntWritable[] { new VIntWritable(4) });
		input.put(new VIntWritable(2), inLinks2); // 2 - 4

		VIntArrayWritable inLinks3 = new VIntArrayWritable();
		inLinks3.set(new VIntWritable[] { new VIntWritable(1) });
		input.put(new VIntWritable(3), inLinks3); // 3 - 1

		VIntArrayWritable inLinks4 = new VIntArrayWritable();
		input.put(new VIntWritable(4), inLinks4); // 4 - <blank>

		// index
		index = new HashMap<VIntWritable, VIntArrayWritable>();
		VIntArrayWritable outLinks1 = new VIntArrayWritable();
		outLinks1.set(new VIntWritable[] { new VIntWritable(3) });
		index.put(new VIntWritable(1), outLinks1);

		VIntArrayWritable outLinks2 = new VIntArrayWritable();
		outLinks2.set(new VIntWritable[] { new VIntWritable(1) });
		index.put(new VIntWritable(2), outLinks2);

		VIntArrayWritable outLinks3 = new VIntArrayWritable();
		index.put(new VIntWritable(3), outLinks3);

		VIntArrayWritable outLinks4 = new VIntArrayWritable();
		outLinks4.set(new VIntWritable[] { new VIntWritable(1),
				new VIntWritable(2) });
		index.put(new VIntWritable(4), outLinks4);

		// debug
		debugInput();
		debugIndex();
		
		// Driver
		mapDriver = new MapDriver<VIntWritable, VIntArrayWritable, VIntWritable, PageRankNode>();
	}

	public void debugInput() {
		for (VIntWritable i : input.keySet()) {
			System.out.printf("%s - %s\n", i, input.get(i));
		}
	}

	public void debugIndex() {
		for (VIntWritable i : index.keySet()) {
			System.out.printf("%s - %s\n", i, index.get(i));
		}
	}

	@Test
	public void test() throws IOException {
		MapperGraph mapper = new MapperGraph();
		mapper.setIndex(index);
		mapDriver.withMapper(mapper);
		for (VIntWritable id : input.keySet()) {
			mapDriver.addInput(id, input.get(id));
		}
		
		List<Pair<VIntWritable, PageRankNode>> outputs = mapDriver.run();

		System.out.println("-------------Graph---------------");
		for (Pair<VIntWritable, PageRankNode> output : outputs) {
			System.out.printf("%s - %s\n", output.getFirst(),
					output.getSecond());
		}
	}

}
