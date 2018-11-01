package wordcount.wordcount;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class wordcount {

	public static class MapperClass extends Mapper<Object, Text, Text, IntWritable> {

		public static String[] stopWordsList = { "a", "about", "above", "after", "again", "against", "all", "am", "an",
				"and", "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below",
				"between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does",
				"doesn't", "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't",
				"has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's",
				"hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if",
				"in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't",
				"my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our",
				"ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's",
				"should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them",
				"themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've",
				"this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd",
				"we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's",
				"which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you",
				"you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves" };
		private Set<String> Stopwords;

		//Intializing the Stopwords list common to all the hadoop mappers 
		public void setup(Context context) {
			Stopwords = new HashSet<String>();
			for (String word : stopWordsList) {
				Stopwords.add(word.replaceAll("[^a-zA-Z]", "").toLowerCase());
			}
		}

		//Hadoop TopNWords Mapper function
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String rawText, cleanText;
			
			Text text = new Text();
			IntWritable one_freq = new IntWritable(1);
			StringTokenizer str_token = new StringTokenizer(value.toString());
			
			while (str_token.hasMoreTokens()) {
				rawText = str_token.nextToken().toString();
				cleanText = rawText.replaceAll("[^a-zA-Z]", "").toLowerCase();
				
				//Condition to eliminate empty strings
				if (!cleanText.isEmpty()) {
					//Condition to eliminate StopWords
					if (!Stopwords.contains(cleanText)) {
						text.set(cleanText);
						context.write(text, one_freq);
					} else {
						continue;
					}

				}
			}
		}
	}
	public static class ReducerClass extends Reducer<Text, IntWritable, Text, LongWritable> {	public void reduce(Text term, Iterable<IntWritable> ones, Context context)
				throws IOException, InterruptedException {
			long count = 0L;
			Iterator<IntWritable> iterator = ones.iterator();
			while (iterator.hasNext()) {
				count++;
				iterator.next();
			}
			context.write(term, new LongWritable(count));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration config = new Configuration();
		String[] Arguments = new GenericOptionsParser(config, args).getRemainingArgs();
		if (Arguments.length != 2) {
			System.err.println("Usage: TopNwords [Input File] [Output Folder]");
			System.exit(2);
		}

		Job HadoopTask = Job.getInstance(config, "Word Count");
		HadoopTask.setJarByClass(wordcount.class);
		HadoopTask.setMapperClass(MapperClass.class);
		HadoopTask.setReducerClass(ReducerClass.class);
		HadoopTask.setNumReduceTasks(4);
		HadoopTask.setOutputKeyClass(Text.class);
		HadoopTask.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(HadoopTask, new Path(Arguments[0]));
		FileOutputFormat.setOutputPath(HadoopTask, new Path(Arguments[1]));
		boolean var = HadoopTask.waitForCompletion(true);
		if (var) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}

}

