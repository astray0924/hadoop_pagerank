package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper.JobInDegreeMapper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper.JobOutDegreeMapper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageMetaNode;

public class TestJobDegree {
	private Pair<Text, PageMetaNode> mapperInput;
	private Map<Text, VIntWritable> index;
	private MapDriver<Text, PageMetaNode, VIntWritable, VIntWritable> driver;
	private Configuration conf;

	@Before
	public void setup() {
		// index
		index = new HashMap<Text, VIntWritable>();
		index.put(new Text("A"), new VIntWritable(1));
		index.put(new Text("B"), new VIntWritable(2));
		index.put(new Text("C"), new VIntWritable(3));
		index.put(new Text("D"), new VIntWritable(4));
		index.put(new Text("E"), new VIntWritable(5));

		// mapper input
		List<String> links = new ArrayList<String>();
		links.add("A");
		links.add("D");
		links.add("E");
		PageMetaNode node = new PageMetaNode(2, "B", links, 3, 1.0f);
		mapperInput = new Pair<Text, PageMetaNode>(node.getTitle(), node);

		// driver
		driver = new MapDriver<Text, PageMetaNode, VIntWritable, VIntWritable>();
		conf = driver.getConfiguration();
		conf.set("wikidump_path", "wikidump");
		conf.set("output_path", "output");
	}

	@Test
	public void testInDegree() throws IOException {
		JobInDegreeMapper job = new JobInDegreeMapper();
		job.setIndex(index);
		driver.withMapper(job).withInput(mapperInput);
		List<Pair<VIntWritable, VIntWritable>> outputs = driver.run();

		System.out.println("-------------InDegree---------------");
		for (Pair<VIntWritable, VIntWritable> output : outputs) {
			System.out.printf("%s - %s\n", output.getFirst(), output.getSecond());
		}
	}

	@Test
	public void testOutDegree() throws IOException {
		JobOutDegreeMapper job = new JobOutDegreeMapper();
		job.setIndex(index);
		driver.withMapper(job).withInput(mapperInput);
		List<Pair<VIntWritable, VIntWritable>> outputs = driver.run();

		System.out.println("-------------OutDegree---------------");
		for (Pair<VIntWritable, VIntWritable> output : outputs) {
			System.out.printf("%s - %s\n", output.getFirst(), output.getSecond());
		}
	}

	@Test
	public void testReducer() {

	}

}