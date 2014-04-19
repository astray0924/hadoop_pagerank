package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util.PathHelper;

public class JobParse {
	public static boolean run(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job = Job.getInstance(conf, "Parse");
		job.setJarByClass(PageRankDriver.class);

		// Set Input Path
		Path inputPath = PathHelper.getPathForName(PathHelper.INPUT_WIKI_DUMP);
		FileInputFormat.addInputPath(job, inputPath);

		// Set Output Path
		Path outputPath = PathHelper.getPathForName(PathHelper.OUTPUT_PARSE);
		FileOutputFormat.setOutputPath(job, outputPath);

		// Mapper
		// job.setMapperClass(cls);
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(PageInfoWritable.class);

		// Reducer
		// job.setReducerClass(cls);
		// job.setOutputKeyClass(theClass);
		// job.setOutputValueClass(theClass);
		// job.setOutputFormatClass(cls);

		return job.waitForCompletion(true);

	}

}
