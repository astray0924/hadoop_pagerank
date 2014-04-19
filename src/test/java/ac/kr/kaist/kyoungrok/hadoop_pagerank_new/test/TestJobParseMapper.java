package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper.JobParseMapper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageMetaNodeWritable;

public class TestJobParseMapper {
	private List<String> pages;

	@Before
	public void setUp() throws IOException {
		// Load the sample page
		pages = new ArrayList<String>();
		InputStream in = getClass().getResourceAsStream("data/sample.data");
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String page = "";
		while ((page = br.readLine()) != null) {
			pages.add(page);
		}
		br.close();
	}

	@Test
	public void testValid() throws IOException {
		// Case 1
		Pair<LongWritable, Text> input1 = new Pair<LongWritable, Text>(
				new LongWritable(1), new Text(pages.get(1)));

		MapDriver<LongWritable, Text, LongWritable, PageMetaNodeWritable> driver = new MapDriver<LongWritable, Text, LongWritable, PageMetaNodeWritable>();
		driver.withMapper(new JobParseMapper()).withInput(input1);

		Pair<LongWritable, PageMetaNodeWritable> output = driver
				.run(true).get(0);
		
		PageMetaNodeWritable node = output.getSecond();
		assertEquals(node.getId(), new VIntWritable(12));
		assertEquals(node.getTitle(), new Text("Anarchism"));
		assertNotNull(node.getOutLinks());
		assertEquals(new VIntWritable(node.getOutLinks().getSize()), node.getOutCount());
	}
}
