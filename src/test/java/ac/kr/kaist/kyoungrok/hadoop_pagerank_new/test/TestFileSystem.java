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
		conf.addResource(getClass().getResourceAsStream("/settings.xml"));

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

	@Test
	public void testPath() {
		fsS3.setWorkingDirectory(new Path(s3));
		System.out.println(fsS3.getWorkingDirectory());
		System.out.println(fsHdfs.getWorkingDirectory());
		fsHdfs.setWorkingDirectory(new Path("./test"));
		System.out.println(fsHdfs.getWorkingDirectory());

		// System.out.println(fsLocal.getWorkingDirectory());
	}
}
