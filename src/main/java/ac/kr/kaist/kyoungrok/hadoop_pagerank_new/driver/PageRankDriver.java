package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.job.JobOutDegree;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.job.JobParse;

public class PageRankDriver extends Configured implements Tool {
	private enum JobName {
		PARSE, OUTDEGREE, INDEGREE, GRAPH, RANK, LIST
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new PageRankDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		// arguments
		if (args.length < 2) {
			System.err
					.printf("Usage: %s [generic options] <wikidump path> <output path> <start job(optional)>\n",
							getClass().getSimpleName());

			System.err.println("<start job>: parse, degree, graph, rank, list");

			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		// Configuration
		Configuration conf = getConf();
		conf.set("wikidump_path", args[0]);
		conf.set("output_path", args[1]);

		// Job selection
		JobName startJob = JobName.PARSE;
		if (args.length >= 3) {
			try {
				startJob = JobName.valueOf(args[2].toUpperCase());
			} catch (IllegalArgumentException e) {

			}
		}

		switch (startJob) {
		case PARSE:
			JobParse.run(conf);
		case OUTDEGREE:
			JobOutDegree.run(conf);
		case GRAPH:
//			System.out.println("graph");
			break;
		case RANK:
		case LIST:
			break;
		default:
			break;
		}

		return 0;
	}
}
