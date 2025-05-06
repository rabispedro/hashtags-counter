package com.brcme.counter.hashtags.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ContaHashtagsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable resultado = new IntWritable();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int soma = 0;
		for (IntWritable value: values) {
			soma += value.get();
		}

		resultado.set(soma);
		context.write(key, resultado);
	}
}
