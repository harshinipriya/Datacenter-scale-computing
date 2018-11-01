package SuccessRate.SuccessRate;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SuccessRate {
	public static class TokenizeMapper1 extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String COMMA_DELIMITER = ",";
			String[] buysDetails = value.toString().split(COMMA_DELIMITER);
			Text wordOut = new Text(buysDetails[2]);
			Text buy = new Text("b");
			context.write(wordOut, buy);
			
		}
	}

	public static class TokenizeMapper2 extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String COMMA_DELIMITER = ",";
			String[] clicksDetails = value.toString().split(COMMA_DELIMITER);
			Text wordOut = new Text(clicksDetails[2]);
			Text click = new Text("c");
			context.write(wordOut, click);
		}
	}

	public static class SumReducer extends Reducer<Text, Text, Text, Text> {
		private TreeMap<Float, ArrayList<Integer>> successRateMap = new TreeMap<Float, ArrayList<Integer>>();

		public void reduce(Text id, Iterable<Text> buysOrClicks, Context context)
				throws IOException, InterruptedException {
			float buysCount = 0;
			float clicksCount = 0;

			for (Text text : buysOrClicks) {
				if (text.toString().equals("b")) {
					buysCount++;
				} else if (text.toString().equals("c")) {
					clicksCount++;
				}
			}

			float successRate = buysCount / clicksCount;

			if (successRateMap.size() < 10) {
				if (successRateMap.get(successRate) == null) {
					successRateMap.put(successRate, new ArrayList<Integer>());
					successRateMap.get(successRate).add(Integer.parseInt(id.toString()));
				} else {
					successRateMap.get(successRate).add(Integer.parseInt(id.toString()));
				}
			} else {
				if (successRateMap.get(successRate) == null) {
					if (successRateMap.firstKey() < successRate) {
						successRateMap.pollFirstEntry();
						successRateMap.put(successRate, new ArrayList<Integer>());
						successRateMap.get(successRate).add(Integer.parseInt(id.toString()));
					}
				} else {
					successRateMap.get(successRate).add(Integer.parseInt(id.toString()));
				}
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			int count = 10;
			while (!successRateMap.isEmpty()) {
				Map.Entry<Float, ArrayList<Integer>> highest = successRateMap.pollLastEntry();
				float key = highest.getKey();
				ArrayList<Integer> ids = highest.getValue();
				Collections.sort(ids);
				for (int id : ids) {
					if (count > 0) {
						context.write(new Text(Integer.toString(id)), new Text(Float.toString(key)));
						count--;
					} else {
						break;
					}
				}
				if (count < 0) {
					break;
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: SuccessRate <buys.txt <clicks.txt <output_directory");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Buy Click");
		job.setJarByClass(SuccessRate.class);
		job.setMapperClass(TokenizeMapper1.class);
		job.setMapperClass(TokenizeMapper2.class);
		job.setReducerClass(SumReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, TokenizeMapper1.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, TokenizeMapper2.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		boolean status = job.waitForCompletion(true);
		if (status) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}
}
