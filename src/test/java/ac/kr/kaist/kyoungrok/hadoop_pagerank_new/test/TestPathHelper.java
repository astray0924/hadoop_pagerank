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
	private String basePath = "file:///Users/kyoungrok/Desktop/";
	
	@Before
	public void setup() {
		conf = new Configuration();
		conf.set("wikidump_path", "file:///Users/kyoungrok/Desktop/wiki-xml/");
		conf.set("output_path", "file:///Users/kyoungrok/Desktop/output/");
	}
	
	@Test
	public void testListFiles() throws IOException {
		Path[] parsedFiles = PathHelper.listDir(PathHelper.getPathByName(PathHelper.NAME_PARSE, conf), conf);
		for (Path p : parsedFiles) {
//			System.out.println(p);
		}
	}
	
	@Test
	public void testGetCacheFiles() throws IOException {
		Path[] caches = PathHelper.getCacheFiles(PathHelper.NAME_TITLE_ID_MAP, conf);
		for (Path p : caches) {
			System.out.println(p);
		}
		
		caches = PathHelper.getCacheFiles(PathHelper.NAME_ID_TITLE_MAP, conf);
		for (Path p : caches) {
			System.out.println(p);
		}
	}
	
	@Test
	public void testOutputPaths() {
		Path parse = PathHelper.getPathByName(PathHelper.NAME_PARSE, conf);
		assertEquals(parse, new Path("file:///Users/kyoungrok/Desktop/output/parse/"));
		
		Path titleIdMap = PathHelper.getPathByName(PathHelper.NAME_TITLE_ID_MAP, conf);
		assertEquals(titleIdMap, new Path("file:///Users/kyoungrok/Desktop/output/parse/titleidmap"));
		
		Path idTitleMap = PathHelper.getPathByName(PathHelper.NAME_ID_TITLE_MAP, conf);
		assertEquals(idTitleMap, new Path("file:///Users/kyoungrok/Desktop/output/parse/idtitlemap"));
		
		Path metanodes = PathHelper.getPathByName(PathHelper.NAME_META_NODES, conf);
		assertEquals(metanodes, new Path("file:///Users/kyoungrok/Desktop/output/parse/metanodes"));
	}
}
