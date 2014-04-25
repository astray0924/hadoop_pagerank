package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.job;

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

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.driver.PageRankDriver;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper.MapperRank;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.reducer.ReducerRank;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util.PathHelper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNode;

public class JobRank {
	public static boolean run(Configuration conf, int K) throws IOException,
			InterruptedException, ClassNotFoundException {

		for (int i = 0; i < K; i++) {
			System.err.printf("[ITERATION] - %s\n", (i + 1));

			Job job = Job.getInstance(conf, "Rank");
			job.setJarByClass(PageRankDriver.class);
			job.setJobName("Rank");

			// Mapper
			job.setMapperClass(MapperRank.class);
			job.setMapOutputKeyClass(VIntWritable.class);
			job.setMapOutputValueClass(PageRankNode.class);

			// Reducer
			job.setReducerClass(ReducerRank.class);
			job.setOutputKeyClass(VIntWritable.class);
			job.setOutputValueClass(PageRankNode.class);

			// File Format
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(MapFileOutputFormat.class);

			// Set Input Path
			Path inputPath = PathHelper.getRankInputPathForIteration(i, conf);
			FileInputFormat.addInputPath(job, inputPath);

			// Set Output Path
			Path outputPath = PathHelper.getRankOutputPathForIteration(i, conf);
			FileOutputFormat.setOutputPath(job, outputPath);

			// Add cache
			Path[] cacheFiles = PathHelper.getRankCacheFilesForIteration(i,
					conf);
			FileSystem fs = FileSystem.get(cacheFiles[0].toUri(), conf);
			for (Path file : cacheFiles) {
				// MapFile이므로 디렉토리만 캐시에 추가해야 함
				if (fs.isDirectory(file)) {
					job.addCacheFile(file.toUri());
				}
			}

			job.waitForCompletion(true);

		}

		return true;

	}
}
