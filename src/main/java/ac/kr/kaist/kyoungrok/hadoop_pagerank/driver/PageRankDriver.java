package ac.kr.kaist.kyoungrok.hadoop_pagerank.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ac.kr.kaist.kyoungrok.hadoop_pagerank.job.JobGraph;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.job.JobInDegree;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.job.JobOutDegree;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.job.JobParse;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.job.JobRank;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.job.JobResult;

public class PageRankDriver extends Configured implements Tool {
	private enum JobName {
		PARSE, DEGREE, GRAPH, RANK, RESULT
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new PageRankDriver(), args);
		System.exit(exitCode);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
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

		int K = 20;
		switch (startJob) {
		case PARSE:
			JobParse.run(conf);
		case DEGREE:
			JobOutDegree.run(conf);
			JobInDegree.run(conf);
		case GRAPH:
			JobGraph.run(conf);
		case RANK:
			JobRank.run(conf, K);
		case RESULT:
			JobResult.run(conf, K);
			break;
		default:
			break;
		}

		return 0;
	}
}
