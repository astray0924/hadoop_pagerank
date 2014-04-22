package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.driver.PageRankDriver;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper.MapperOutDegree;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.reducer.ReducerDegree;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util.PathHelper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.VIntArrayWritable;

public class JobOutDegree {
	public static boolean run(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job = Job.getInstance(conf, "OutDegree");
		job.setJarByClass(PageRankDriver.class);
		job.setJobName("OutDegree");

		// Set Input Path
		Path inputPath = PathHelper.getPathByName(PathHelper.NAME_META_NODES,
				conf);
		FileInputFormat.addInputPath(job, inputPath);

		// Set Output Path
		Path outputPath = PathHelper.getPathByName(PathHelper.NAME_OUT_DEGREE,
				conf);
		FileOutputFormat.setOutputPath(job, outputPath);

		// Mapper
		job.setMapperClass(MapperOutDegree.class);
		job.setMapOutputKeyClass(VIntWritable.class);
		job.setMapOutputValueClass(VIntWritable.class);

		// Reducer
		job.setReducerClass(ReducerDegree.class);
		job.setOutputKeyClass(VIntWritable.class);
		job.setOutputValueClass(VIntArrayWritable.class);
		
		// File Format
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(MapFileOutputFormat.class);

		// Add cache
		Path[] cacheFiles = PathHelper.getCacheFiles(
				PathHelper.NAME_TITLE_ID_MAP, conf);
		for (Path file : cacheFiles) {
			job.addCacheFile(file.toUri());
		}

		return job.waitForCompletion(true);

	}

}
