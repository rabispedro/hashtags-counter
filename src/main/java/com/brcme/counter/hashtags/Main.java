package com.brcme.counter.hashtags;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import com.brcme.counter.hashtags.mappers.ContaHashtagsMapper;
import com.brcme.counter.hashtags.reducers.ContaHashtagsReducer;

public class Main {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "conta hashtags");

		job.setJarByClass(Main.class);
		job.setMapperClass(ContaHashtagsMapper.class);
		job.setReducerClass(ContaHashtagsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
