package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import ac.kr.kaist.kyoungrok.hadoop_pagerank.mapper.MapperInDegree;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.mapper.MapperOutDegree;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.reducer.ReducerDegree;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.util.PathHelper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.writable.PageMetaNode;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.writable.VIntArrayWritable;

public class TestJobDegree {
	private Pair<Text, PageMetaNode> mapperInput;
	private Map<Text, VIntWritable> index;
	private MapDriver<Text, PageMetaNode, VIntWritable, VIntWritable> mapDriver;
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
		mapDriver = new MapDriver<Text, PageMetaNode, VIntWritable, VIntWritable>();
		conf = mapDriver.getConfiguration();
		conf.set("wikidump_path", "wikidump");
		conf.set("output_path", "output");
	}

	@Test
	public void testOnRealData() throws IOException {
		MapperOutDegree mapper = new MapperOutDegree();

		// set cache
		Path[] cacheFiles = PathHelper.getCacheFiles(
				PathHelper.NAME_TITLE_ID_MAP, conf);
		for (Path p : cacheFiles) {
			mapDriver.withMapper(mapper).addCacheFile(p.toUri());
		}

		// set input path
		mapDriver.setMapInputPath(PathHelper.getPathByName(
				PathHelper.NAME_META_NODES, conf));
		
//		mapDriver.run();
	}

	@Test
	public void testInDegree() throws IOException {
		MapperInDegree job = new MapperInDegree();
		job.setIndex(index);
		mapDriver.withMapper(job).withInput(mapperInput);
		List<Pair<VIntWritable, VIntWritable>> outputs = mapDriver.run();

		System.out.println("-------------InDegree---------------");
		for (Pair<VIntWritable, VIntWritable> output : outputs) {
			System.out.printf("%s - %s\n", output.getFirst(),
					output.getSecond());
		}
	}

	@Test
	public void testOutDegree() throws IOException {
		MapperOutDegree job = new MapperOutDegree();
		job.setIndex(index);
		mapDriver.withMapper(job).withInput(mapperInput);
		List<Pair<VIntWritable, VIntWritable>> outputs = mapDriver.run();

		System.out.println("-------------OutDegree---------------");
		for (Pair<VIntWritable, VIntWritable> output : outputs) {
			System.out.printf("%s - %s\n", output.getFirst(),
					output.getSecond());
		}
	}

	@Test
	public void testReducer() throws IOException {
		ReducerDegree reducer = new ReducerDegree();
		ReduceDriver<VIntWritable, VIntWritable, VIntWritable, VIntArrayWritable> driver = new ReduceDriver<VIntWritable, VIntWritable, VIntWritable, VIntArrayWritable>();
		driver.withReducer(reducer).withInput(
				new VIntWritable(1),
				Arrays.asList(new VIntWritable(1), new VIntWritable(4),
						new VIntWritable(3), new VIntWritable(2)));

		List<Pair<VIntWritable, VIntArrayWritable>> outputs = driver.run();

		System.out.println("-------------Reducer---------------");
		for (Pair<VIntWritable, VIntArrayWritable> output : outputs) {
			System.out.printf("%s - %s\n", output.getFirst(),
					output.getSecond());
		}
	}

}
