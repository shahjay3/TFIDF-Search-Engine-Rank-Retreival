package org.myorg;


import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/*
 * Jay Shah
 * PID: 800326050
 * jdshah@uncc.edu
 */
public class TFIDF extends Configured implements Tool {
    /* in this program, the user will submit a file input path, an internmediate path, a output path, number of documents, space separated. */
	private static final Logger LOG = Logger.getLogger(TFIDF.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TFIDF(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " tfidf ");
		job.setJarByClass(this.getClass());
        // the first args array element is the file input path
		FileInputFormat.addInputPaths(job, args[0]);
		// the second args aray is the output path (which for the first map-reduce is the intermediate path).
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// set the first job's map-reduce jobs, map before reduce
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
         // check to see if the first job was  asuccess
		int success = job.waitForCompletion(true)? 0:1;
		// a successful job means that success will be zero, if the job is successful then go into job 2
		if (success==0) {
			// we get an instance of job 2, call it secondMapReduce
			Job j = Job.getInstance(getConf(), "MR2");
			Configuration conf = j.getConfiguration();
			conf.set("user_set_document_numbers", args[3]);
		    
			j.setJarByClass(this.getClass());
			// Set the mapper 2 and reducer 2 class
			j.setMapperClass(Mapper2.class);
			j.setReducerClass(Reducer2.class);
			
			j.setJarByClass(this.getClass());
	         // this time, the input file will be the internmediate output file and the final output file will be the last arguement
			FileInputFormat.addInputPath(j, new Path(args[1]));
			FileOutputFormat.setOutputPath(j, new Path(args[2]));
			
			//outputKeyClass is at text type this will set the type for both the mapper and reduce
			j.setOutputKeyClass(Text.class);
			// output valueclass is a doublewritable this will set the type for both the mapper and reducer
			j.setOutputValueClass(DoubleWritable.class);
			// now we change the outputkeyclass outputvalueclass for mapper and reducer. 
			j.setMapOutputKeyClass(Text.class);
			j.setMapOutputValueClass(Text.class);
			
			// check to see if job 2 finished
            success = j.waitForCompletion(true) ? 0 : 1;
		}

		return success;
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		private final static DoubleWritable one = new DoubleWritable(1);
		private Text word = new Text();

		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
            // we get a line text, convert it to a string
			String line = lineText.toString();
			Text currentWord = new Text();
              // split the line on the word boundary (tabs, spaces)
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				//get the name of the file
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				String filename = fileSplit.getPath().getName();
				// 1. lowercase the word, 2. concatenate the file name and ##### to the word to get the final current word
				currentWord = new Text(word.toLowerCase() + "#####" + filename);
				// send the current word and the value of 1
				context.write(currentWord, one);
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<DoubleWritable> counts,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			// for each count in the iterable counts, get the value and add it to sum. we are adding up the number of times the word appears
			for (DoubleWritable count : counts) {
				sum += count.get();
			}
			
			// get the term frequency using the below equation
			double sum2 = 1.0 + Math.log10(Double.valueOf(sum));
			// convert the sum into a double writable
			context.write(word, new DoubleWritable(sum2));
		}
	}

	public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text word = new Text();
		
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString();
			Text currentWord = new Text();
			// split the input by the hashtags
                String [] lineArray = line.split("#####");
               // rearrange to the indicated format in assignment (word, filename.txt = termfrequency)
				currentWord = new Text(lineArray[0]);
				lineArray[1] = lineArray[1].replaceAll("\\s+", "=");
				//send this (word, filename = TF) format out. 
				context.write(currentWord, new Text(lineArray[1]));
			}
		}
	

	public static class Reducer2 extends Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			// total number of documents are given by the user, set that equal to total documents, for IDF calculation
			int total_documents = Integer.valueOf(context.getConfiguration().get("user_set_document_numbers"));
		
			
			// create a hashmap to store the <filename, TF value pairs> that exist in the postings list for the current word
			HashMap<String, String> hash_map = new HashMap<String, String>();

			for (Text val : values) {
				// split the current value by the equals 
				String [] text_line_array = val.toString().split("=");
				// put the filename, tf value pair and put it into the hashmap
				hash_map.put(text_line_array[0], text_line_array[1]);
				// incrment sum, this is how we get the lenght of the postings list
				sum++;
				}
			for (String fileName : hash_map.keySet()) {
				//using equation 3 we calculate IDF using the totla number of docuemnts lengght of the postings list
				double IDF = Math.log10( 1.0 + Double.valueOf(total_documents) / Double.valueOf(sum ) );
				// access the TF value using the fileName Key
				double TF = Double.valueOf(hash_map.get(fileName));

				
				
				context.write(new Text (word.toString() + "#####" + fileName ), new DoubleWritable(TF * IDF));
				
				}
		}
	}
}