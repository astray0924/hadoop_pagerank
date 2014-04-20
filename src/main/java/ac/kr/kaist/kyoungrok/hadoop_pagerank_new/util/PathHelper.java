package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class PathHelper {
	public static final String NAME_PARSE = "parse";
	public static final String NAME_META_NODE = "metanodes";
	public static final String NAME_TITLE_ID_MAP = "titleidmap";
	public static final String NAME_ID_TITLE_MAP = "idtitlemap";
	
	public static Path getPathForName(String pathName) {
		return new Path(pathName);
	}
	
	public static Path[] listDir(Path dirPath, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(dirPath.toUri(), conf);

		return FileUtil.stat2Paths(fs.listStatus(dirPath));
	}
}
