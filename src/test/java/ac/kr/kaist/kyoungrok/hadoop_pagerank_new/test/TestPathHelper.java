package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util.PathHelper;

public class TestPathHelper {
	private Configuration conf;
	
	@Before
	public void setup() {
		conf = new Configuration();
		conf.set("wikidump_path", "hdfs://localhost:9000/wiki-xml/");
		conf.set("output_path", "file:///Users/kyoungrok.jang/Desktop/data/");
	}
	
	@Test
	public void testListFiles() throws IOException {
		Path[] parsedFiles = PathHelper.listDir(PathHelper.getPathForName(PathHelper.NAME_PARSE, conf), conf);
		for (Path p : parsedFiles) {
			System.out.println(p);
		}
	}
	
	@Test
	public void testOutputPaths() {
		Path parse = PathHelper.getPathForName(PathHelper.NAME_PARSE, conf);
		assertEquals(parse, new Path("hdfs://localhost:9000/output/parse/"));
		
		Path titleIdMap = PathHelper.getPathForName(PathHelper.NAME_TITLE_ID_MAP, conf);
		assertEquals(titleIdMap, new Path("hdfs://localhost:9000/output/parse/titleidmap"));
		
		Path idTitleMap = PathHelper.getPathForName(PathHelper.NAME_ID_TITLE_MAP, conf);
		assertEquals(idTitleMap, new Path("hdfs://localhost:9000/output/parse/idtitlemap"));
		
		Path metanodes = PathHelper.getPathForName(PathHelper.NAME_META_NODE, conf);
		assertEquals(metanodes, new Path("hdfs://localhost:9000/output/parse/metanodes"));
	}
}
