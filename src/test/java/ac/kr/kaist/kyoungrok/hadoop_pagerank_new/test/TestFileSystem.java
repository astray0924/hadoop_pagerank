package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.test;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class TestFileSystem {
	private String local = "file:///Users/kyoungrok/";
	private String s3 = "s3://wiki-xml/";
	private String hdfs = "hdfs://localhost:9000/sample_output/";

	private Configuration conf;
	private FileSystem fsDefault;
	private FileSystem fsLocal;
	private FileSystem fsS3;
	private FileSystem fsHdfs;

	@Before
	public void setUp() throws IOException {
		conf = new Configuration();

		fsDefault = FileSystem.get(conf);
		fsS3 = FileSystem.get(URI.create(s3), conf);
		fsLocal = FileSystem.get(URI.create(local), conf);
		fsHdfs = FileSystem.get(URI.create(hdfs), conf);
	}

	@Test
	public void testFileSystemScheme() throws IOException {
		System.out.println(fsDefault.getScheme());
		System.out.println(fsLocal.getScheme());
		System.out.println(fsS3.getScheme());
		System.out.println(fsHdfs.getScheme());
	}

	// Configuration conf = new Configuration();
	// conf.addResource("configuration-1.xml");
	// assertThat(conf.get("color"), is("yellow"));
	// assertThat(conf.getInt("size", 0), is(10));
	// assertThat(conf.get("breadth", "wide"), is("wide"));
	
	@Test
	public void testPath() {
//		System.out.println(fsS3.setWorkingDirectory(new_dir););
		System.out.println(fsHdfs.getWorkingDirectory());
		fsHdfs.setWorkingDirectory(new Path("./test"));
		System.out.println(fsHdfs.getWorkingDirectory());
		
//		System.out.println(fsLocal.getWorkingDirectory());
	}
}
