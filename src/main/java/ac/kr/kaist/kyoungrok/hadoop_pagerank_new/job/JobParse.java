package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.driver.PageRankDriver;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.mapper.MapperParse;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.reducer.ReducerParse;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util.PathHelper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageMetaNode;

public class JobParse {
	public static boolean run(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job = Job.getInstance(conf, "Parse");
		job.setJarByClass(PageRankDriver.class);
		job.setJobName("Parse");

		// Set Input Path
		FileInputFormat.addInputPath(job, new Path(conf.get("wikidump_path")));

		// Set Output Path
		Path outputPath = PathHelper.getPathByName(PathHelper.NAME_PARSE, conf);
		FileOutputFormat.setOutputPath(job, outputPath);

		// Mapper
		job.setMapperClass(MapperParse.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageMetaNode.class);

		// Reducer
		job.setReducerClass(ReducerParse.class);
		MultipleOutputs.addNamedOutput(job, PathHelper.NAME_META_NODES,
				SequenceFileOutputFormat.class, Text.class, PageMetaNode.class);
		MultipleOutputs.addNamedOutput(job, PathHelper.NAME_TITLE_ID_MAP,
				SequenceFileOutputFormat.class, Text.class, VIntWritable.class);
		MultipleOutputs.addNamedOutput(job, PathHelper.NAME_ID_TITLE_MAP,
				SequenceFileOutputFormat.class, VIntWritable.class, Text.class);
		
		// File Format
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		return job.waitForCompletion(true);

	}

}
