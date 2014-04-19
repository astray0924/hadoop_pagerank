package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRankDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new PageRankDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		// arguments
		if (args.length < 2) {
			System.err.printf(
					"Usage: %s [generic options] <input path> <output path>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		// Configuration
		Configuration conf = getConf();
		
		// Parse 
		JobParse.run(conf);

		return 0;
	}
}
