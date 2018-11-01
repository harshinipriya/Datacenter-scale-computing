package ItemClick.ItemClick;


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

public class ItemClick {
	public static class TokenizeMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] buysDetails = value.toString().split(",");
			String month = buysDetails[1].split("-")[1];
			if(month.equals("04")){
				context.write(new Text(buysDetails[2]), new Text("one"));
			}
		}
	}

	public static class SumReducer extends Reducer<Text, Text, Text, Text> {
		private TreeMap<Integer, ArrayList<Integer>> clicksMap = new TreeMap<Integer, ArrayList<Integer>>();
		
		public void reduce(Text id, Iterable<Text> ones, Context context)
				throws IOException, InterruptedException {
			int clicks = 0;
			Iterator<Text> iterator = ones.iterator();
			while (iterator.hasNext()) {
				clicks++;
				iterator.next();
			}
			
			if (clicksMap.size() < 10) {
				if (clicksMap.get(clicks) == null) {
					clicksMap.put(clicks, new ArrayList<Integer>());
					clicksMap.get(clicks).add(Integer.parseInt(id.toString()));
				} else {
					clicksMap.get(clicks).add(Integer.parseInt(id.toString()));
				}
			} else {
				if (clicksMap.get(clicks) == null) {
					if (clicksMap.firstKey() < clicks) {
						clicksMap.pollFirstEntry();
						clicksMap.put(clicks, new ArrayList<Integer>());
						clicksMap.get(clicks).add(Integer.parseInt(id.toString()));
					}
				} else {
					clicksMap.get(clicks).add(Integer.parseInt(id.toString()));
				}
			}
			
			
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			int count = 10;
			while (!clicksMap.isEmpty()) {
				Map.Entry<Integer, ArrayList<Integer>> highest = clicksMap.pollLastEntry();
				int key = highest.getKey();
				ArrayList<Integer> ids = highest.getValue();
				Collections.sort(ids);
				for (int id : ids) {
					if (count > 0) {
						context.write(new Text(Integer.toString(id)), new Text(Integer.toString(key)));
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
		if (otherArgs.length != 2) {
			System.err.println("Usage: itemClick <clicks.txt <output_directory");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "itemClick");
		job.setJarByClass(ItemClick.class);
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


