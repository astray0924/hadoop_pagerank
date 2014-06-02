package ac.kr.kaist.kyoungrok.hadoop_pagerank.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;

import ac.kr.kaist.kyoungrok.hadoop_pagerank.driver.PageRankDriver;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.mapper.MapperGraph;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.reducer.ReducerGraph;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.util.PathHelper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank.writable.PageRankNode;

public class JobGraph {
	public static boolean run(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job = Job.getInstance(conf, "InDegree");
		job.setJarByClass(PageRankDriver.class);
		job.setJobName("InDegree");

		// Set Input Path
		Path inputPath = PathHelper.getPathByName(PathHelper.NAME_IN_DEGREE,
				conf);
		FileInputFormat.addInputPath(job, inputPath);

		// Set Output Path
		Path outputPath = PathHelper.getPathByName(PathHelper.NAME_GRAPH, conf);
		FileOutputFormat.setOutputPath(job, outputPath);

		// Mapper
		job.setMapperClass(MapperGraph.class);
		job.setMapOutputKeyClass(VIntWritable.class);
		job.setMapOutputValueClass(PageRankNode.class);

		// Reducer
		job.setReducerClass(ReducerGraph.class);
		job.setOutputKeyClass(VIntWritable.class);
		job.setOutputValueClass(PageRankNode.class);

		// File Format
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(MapFileOutputFormat.class);

		// Add cache
		Path[] cacheFiles = PathHelper.getCacheFiles(PathHelper.NAME_OUT_DEGREE,
				conf);
		FileSystem fs = FileSystem.get(cacheFiles[0].toUri(), conf);
		for (Path file : cacheFiles) {
			// MapFile이므로 디렉토리만 캐시에 추가해야 함
			if (fs.isDirectory(file)) {
				job.addCacheFile(file.toUri());
			}
		}

		return job.waitForCompletion(true);

	}
}
