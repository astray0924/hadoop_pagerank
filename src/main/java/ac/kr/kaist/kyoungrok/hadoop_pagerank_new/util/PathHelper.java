package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util;

import org.apache.hadoop.fs.Path;

public class PathHelper {
	public static final String PATH_NAME_OUTPUT_PARSE = "parse";
	
	public static Path getPathForName(String pathName) {
		return new Path(pathName);
	}
}
