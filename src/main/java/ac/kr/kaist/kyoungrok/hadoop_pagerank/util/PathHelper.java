package ac.kr.kaist.kyoungrok.hadoop_pagerank.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class PathHelper {
	private enum PathName {
		NONE, PARSE, METANODES, TITLEIDMAP, IDTITLEMAP, OUTDEGREE, INDEGREE, GRAPH, RANK, RESULT
	}

	public static final String NAME_PARSE = "parse";
	public static final String NAME_META_NODES = "metanodes";
	public static final String NAME_TITLE_ID_MAP = "titleidmap";
	public static final String NAME_ID_TITLE_MAP = "idtitlemap";
	public static final String NAME_OUT_DEGREE = "outdegree";
	public static final String NAME_IN_DEGREE = "indegree";
	public static final String NAME_GRAPH = "graph";
	public static final String NAME_RANK = "rank";
	public static final String NAME_RESULT = "result";

	private static final Path emptyPath = new Path(" ");

	public static Path getRankInputPathForIteration(int k, Configuration conf) {
		if (k == 0) {
			return getPathByName(NAME_GRAPH, conf);
		} else {
			return new Path(getPathByName(NAME_RANK, conf), String.valueOf(k));
		}

	}

	public static Path getRankOutputPathForIteration(int k, Configuration conf) {
		Path rankPath = getPathByName(NAME_RANK, conf);
		return new Path(rankPath, String.valueOf(k + 1));
	}

	public static Path getPathByName(String pathName, Configuration conf) {
		Path basePath = new Path(conf.get("output_path"));
		PathName pName = PathName.NONE;

		try {
			pName = PathName.valueOf(pathName.toUpperCase());
		} catch (IllegalArgumentException e) {
			System.err.println(e.getMessage());
			return emptyPath;
		}

		switch (pName) {
		case PARSE:
			// return new Path(basePath, new Path(NAME_PARSE));
			return new Path(NAME_PARSE);
		case METANODES:
			// return new Path(new Path(basePath, new Path(NAME_PARSE)), new
			// Path(
			// NAME_META_NODES));
			return new Path(new Path(NAME_PARSE), new Path(NAME_META_NODES));
		case TITLEIDMAP:
			// return new Path(new Path(basePath, new Path(NAME_PARSE)), new
			// Path(
			// NAME_TITLE_ID_MAP));
			return new Path(new Path(NAME_PARSE), new Path(NAME_TITLE_ID_MAP));
		case IDTITLEMAP:
			// return new Path(new Path(basePath, new Path(NAME_PARSE)), new
			// Path(
			// NAME_ID_TITLE_MAP));
			return new Path(new Path(NAME_PARSE), new Path(NAME_ID_TITLE_MAP));
		case OUTDEGREE:
			// return new Path(basePath, new Path(NAME_OUT_DEGREE));
			return new Path(NAME_OUT_DEGREE);
		case INDEGREE:
			// return new Path(basePath, new Path(NAME_IN_DEGREE));
			return new Path(NAME_IN_DEGREE);
		case GRAPH:
			// return new Path(basePath, new Path(NAME_GRAPH));
			return new Path(NAME_GRAPH);
		case RANK:
			// return new Path(basePath, new Path(NAME_RANK));
			return new Path(NAME_RANK);
		case RESULT:
			return new Path(basePath, new Path(NAME_RESULT));
		default:
			return emptyPath;
		}
	}

	public static FileSystem getFileSystem(Path path, Configuration conf)
			throws IOException {
		return FileSystem.get(path.toUri(), conf);
	}

	public static Path[] listFiles(Path dirPath, Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(dirPath.toUri(), conf);
		return FileUtil.stat2Paths(fs.listStatus(dirPath));
	}

	public static Path[] getRankCacheFilesForIteration(int k, Configuration conf)
			throws IOException {
		Path path = PathHelper.getRankInputPathForIteration(k, conf);
		return listFiles(path, conf);
	}

	public static Path[] getCacheFiles(String cacheName, Configuration conf)
			throws IOException {
		PathName name = PathName.NONE;

		try {
			name = PathName.valueOf(cacheName.toUpperCase());
		} catch (IllegalArgumentException e) {
			System.err.println(e.getMessage());
			return new Path[0];
		}

		Path cachePath = new Path(" ");
		switch (name) {
		case TITLEIDMAP:
			cachePath = getPathByName(NAME_TITLE_ID_MAP, conf);
			return listFiles(cachePath, conf);
		case IDTITLEMAP:
			cachePath = getPathByName(NAME_ID_TITLE_MAP, conf);
			return listFiles(cachePath, conf);
		case OUTDEGREE:
			cachePath = getPathByName(NAME_OUT_DEGREE, conf);
			return listFiles(cachePath, conf);
		case GRAPH:
			cachePath = getPathByName(NAME_GRAPH, conf);
			return listFiles(cachePath, conf);
		default:
			break;
		}

		return new Path[0];
	}

	public static Path[] uris2Paths(URI[] uris) {
		Path[] paths = new Path[uris.length];

		for (int i = 0; i < uris.length; i++) {
			paths[i] = new Path(uris[i]);
		}

		return paths;
	}
}
