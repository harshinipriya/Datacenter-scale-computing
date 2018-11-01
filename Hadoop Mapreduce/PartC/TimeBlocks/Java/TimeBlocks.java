package TimeBlocks.TimeBlocks;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TimeBlocks {
	public static class TokenizeMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] buysDetails = value.toString().split(",");
			String time = buysDetails[1].split("T")[1].split(":")[0];
			int amount = Integer.parseInt(buysDetails[3]) * Integer.parseInt(buysDetails[4]);
			context.write(new Text(time), new Text(Integer.toString(amount)));
		}
	}

	public static class SumReducer extends Reducer<Text, Text, Text, Text> {
		private TreeMap<Integer, ArrayList<Integer>> revenueMap = new TreeMap<Integer, ArrayList<Integer>>();

		public void reduce(Text id, Iterable<Text> amounts, Context context) throws IOException, InterruptedException {
			int revenue = 0;
			Iterator<Text> iterator = amounts.iterator();
			while (iterator.hasNext()) {
				revenue += Integer.parseInt(iterator.next().toString());
			}

			if (revenueMap.get(revenue) == null) {
				revenueMap.put(revenue, new ArrayList<Integer>());
				revenueMap.get(revenue).add(Integer.parseInt(id.toString()));
			} else {
				revenueMap.get(revenue).add(Integer.parseInt(id.toString()));
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			while (!revenueMap.isEmpty()) {
				Map.Entry<Integer, ArrayList<Integer>> highest = revenueMap.pollLastEntry();
				int k = highest.getKey();
				ArrayList<Integer> ids = highest.getValue();
				Collections.sort(ids);
				for (int i : ids) {
					String key = Integer.toString(k);
					String id;
					if(i<10) {
						id = "";
						id = "0"+Integer.toString(i);
					}
					else {
						id = "";
						id = Integer.toString(i);
					}
						context.write(new Text(id), new Text(key));
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: timeBlocks <input_file <output_directory");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "timeeeBlocks");
		job.setJarByClass(TimeBlocks.class);
		job.setMapperClass(TokenizeMapper.class);
		job.setReducerClass(SumReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		boolean status = job.waitForCompletion(true);
		if (status) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}
}


