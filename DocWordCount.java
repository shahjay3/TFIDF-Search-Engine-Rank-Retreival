package org.myorg;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/*
 * Jay Shah
 * jdshah@uncc.edu
 * PID 800326050
 */
public class DocWordCount extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( DocWordCount.class);
  
   
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new DocWordCount(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " docwordcount ");
      job.setJarByClass( this .getClass());
      // the following two lines gets the first arguemnt and second arugment in args array to get the input and output files
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      // first set the mapper job
      job.setMapperClass( Map .class);
      // then set the reducer job
      job.setReducerClass( Reduce .class);
      // the output key class for both the mapper and reduce are set at Text f
      job.setOutputKeyClass( Text .class);
      // the output value class for both the mapper and reducer is set to IntWritable. 
      job.setOutputValueClass( IntWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      // the variable "one" is the set to value of 1, the reason is because mapper will output each word as soon as it is encountered
	   // the word will be count 1. 
	   private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");
      // this method takes in the input from the file from 
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
          // take lineText and turn it into a string
         String line  = lineText.toString();
         Text currentWord  = new Text();
           //split the line according to tabs, spaces etc..
         for ( String word  : WORD_BOUNDARY .split(line)) {
        	 // if the word is empty then continue
            if (word.isEmpty()) {
               continue;
            }
            // we are going to get the file name from the context object by first casting it to a filesplit object
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            // use the get name method from the path to get the name of the file for the word
            String filename = fileSplit.getPath().getName();
            // the word needs to be all lower cased, and then concetenated with the hashtag and file name as the key
            currentWord  = new Text(word.toLowerCase() +"#####"+filename);
            //output the word (with hashtags and file name) and the value of 1
            context.write(currentWord,one);
         }
      }
   }
   // the input to this class will be the word (with hashtage + file name) and the conts list ( [1,1,1,1..] )
   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
         // we add up the one's in the counts list
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
         // we output the word along with the word count. 
         context.write(word,  new IntWritable(sum));
      }
   }
}
