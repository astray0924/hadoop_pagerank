package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.driver.PageRankDriver;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper.JobParseMapper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.reducer.JobParseReducer;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util.PathHelper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageMetaNodeWritable;

public class JobParse {
	public static boolean run(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job = Job.getInstance(conf, "Parse");
		job.setJarByClass(PageRankDriver.class);

		// Set Input Path
		Path inputPath = PathHelper.getPathForName("none"); // TODO 수정 필요
		FileInputFormat.addInputPath(job, new Path(conf.get("global_input")));

		// Set Output Path
		Path outputPath = PathHelper
				.getPathForName(PathHelper.PATH_NAME_OUTPUT_PARSE);
		FileOutputFormat.setOutputPath(job, new Path(conf.get("global_output")));

		// Mapper
		job.setMapperClass(JobParseMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageMetaNodeWritable.class);

		// Reducer
		job.setReducerClass(JobParseReducer.class);

		MultipleOutputs.addNamedOutput(job, "metanodes",
				SequenceFileOutputFormat.class, Text.class, PageMetaNodeWritable.class);
		MultipleOutputs.addNamedOutput(job, "titleidmap",
				SequenceFileOutputFormat.class, Text.class, VIntWritable.class);
		MultipleOutputs.addNamedOutput(job, "idtitlemap",
				SequenceFileOutputFormat.class, VIntWritable.class, Text.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		return job.waitForCompletion(true);

	}

}
