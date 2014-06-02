package ac.kr.kaist.kyoungrok.hadoop_pagerank.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import ac.kr.kaist.kyoungrok.hadoop_pagerank.driver.PageRankDriver;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.mapper.MapperResult;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.reducer.ReducerResult;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.util.PathHelper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.writable.PageRankResult;

public class JobResult {
	public static boolean run(Configuration conf, int k) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job = Job.getInstance(conf, "Result");
		job.setJarByClass(PageRankDriver.class);
		job.setJobName("Result");

		// Set Input Path
		Path inputPath = PathHelper.getRankOutputPathForIteration(k - 1, conf);
		FileInputFormat.addInputPath(job, inputPath);

		// Set Output Path
		Path outputPath = PathHelper
				.getPathByName(PathHelper.NAME_RESULT, conf);
		FileOutputFormat.setOutputPath(job, outputPath);

		// Mapper
		job.setMapperClass(MapperResult.class);
		job.setMapOutputKeyClass(PageRankResult.class);
		job.setMapOutputValueClass(NullWritable.class);

		// Reducer
		job.setReducerClass(ReducerResult.class);
		job.setOutputKeyClass(PageRankResult.class);
		job.setOutputValueClass(NullWritable.class);

		// File Format
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// add cache files
		Path[] cacheFiles = PathHelper.getCacheFiles(
				PathHelper.NAME_ID_TITLE_MAP, conf);
		for (Path file : cacheFiles) {
			job.addCacheFile(file.toUri());
		}

		return job.waitForCompletion(true);

	}
}
