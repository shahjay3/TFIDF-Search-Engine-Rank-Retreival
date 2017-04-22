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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.lang.Math;
import org.apache.hadoop.io.DoubleWritable;

/* Jay Shah
 * PID : 800326050
 * jdshah@uncc.edu
 */
public class TermFrequency extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TermFrequency.class);
   
   
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TermFrequency(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	   // set an instance of job
      Job job  = Job .getInstance(getConf(), " termfrequency ");
      job.setJarByClass( this .getClass());
      // the following sets the input path, output path and the flow of the map reduce program
      
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      
     // it sets map job then reduce jobs
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      
     // the setOutputKeyClass and setOutputValueClass sets the types for the output of the map and reduce jobs. 
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( DoubleWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
      
	   // this variable denotes the count of each word as it enters the mapper, which is one
	   private final static DoubleWritable one  = new DoubleWritable( 1);
      private Text word  = new Text();
      
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
         // turn the lineText to a string type
         String line  = lineText.toString();
         Text currentWord  = new Text();
          // split the line according to the word boundary
         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
            // for each word, find the file it came from
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            // this gets the name of the file it came from 
            String filename = fileSplit.getPath().getName();
            // for the current word,we concatenate the filename in the formate : word#####fileName.txt
            currentWord  = new Text(word.toLowerCase() +"#####"+filename);
            // send the word with a count of one to the context instance. 
            context.write(currentWord,one);
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
         // for each count in the counts iterable, we sum up the value. this is basically just adding up the [1,1,1,1] array of each word. 
         for ( DoubleWritable count  : counts) {
            sum  += count.get();
         }
         // we now find the Term Frequency using the below equation and send it to output.
         double sum2 = 1.0+Math.log10(Double.valueOf(sum));
         context.write(word,  new DoubleWritable(sum2));
      }
   }
}
