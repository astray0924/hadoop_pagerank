package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util;

import org.apache.hadoop.fs.Path;

public class PathHelper {
	public static final String NAME_PARSE = "parse";
	public static final String NAME_META_NODE = "metanodes";
	public static final String NAME_TITLE_ID_MAP = "titleidmap";
	public static final String NAME_ID_TITLE_MAP = "idtitlemap";
	
	public static Path getPathForName(String pathName) {
		return new Path(pathName);
	}
}
