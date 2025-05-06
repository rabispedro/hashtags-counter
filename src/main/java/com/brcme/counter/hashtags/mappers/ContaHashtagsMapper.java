package com.brcme.counter.hashtags.mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ContaHashtagsMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static final IntWritable UM = new IntWritable(1);
	private final Text palavra = new Text();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer stringTokenizer = new StringTokenizer(value.toString());

		while (stringTokenizer.hasMoreTokens()) {
			String token = stringTokenizer.nextToken();

			if (token.startsWith("#")) {
				palavra.set(token.toLowerCase().replaceAll("[^a-zA-Z# ]", ""));
				context.write(palavra, UM);
			}
		}
	}
}
