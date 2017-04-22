package org.myorg;

import java.io.File;
import java.io.IOException;
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
import java.lang.Math;
import org.apache.hadoop.io.DoubleWritable;

/*
 * Jay Shah
 * PID : 800326050
 * jdshah@uncc.edu
 */

public class Search extends Configured implements Tool {
// in this file, the user specifies the input file (which will be the final output file that was produced in TFIDF, the ouput file path and the query in the foramt "word1 word2..."
	// this will be done in the command line
   private static final Logger LOG = Logger .getLogger( Search.class);
   // make a new global args array, in order for it to be accessed in other methods. 
   
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Search(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " search ");
      job.setJarByClass( this .getClass());
      // put in the input path
      FileInputFormat.addInputPaths(job,  args[0]);
      // put in the output path
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      
      Configuration conf = job.getConfiguration();
      // get the user query,make it a variable called "queryArray"
	  conf.set("searchArg", args[2]);
	  // set the map and reduce jobs
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      //the key class for both the outputs will be Text
      job.setOutputKeyClass( Text .class);
      // the output value class for both will be double writable. 
      job.setOutputValueClass( DoubleWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
     
      private Text word  = new Text();

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  // get the search string from the command line
    	String search_string = context.getConfiguration().get("searchArg");
    	 // split this search string on spaces
    	 String [] search_string_array = search_string.split("\\s+");
    	 // for the file input, each line will be converted to a string
    	 String line = lineText.toString();
    	 // for the file input, each line will be split by the hashtag
         String[] lineArray = line.split("#####");
         // lineArray[1] holds the tfIDF score and filenAME, so split it based on spaces
         String[] file_and_tfidf = lineArray[1].split("\\s+");
         //now we compare the search string witht he word
    	  for(int i=0; i<search_string_array.length;i++){
    		  // lineArray[0] is the word, so we compare it to every word in the search string
    		  if(lineArray[0].equals(search_string_array[i])){ 
    			  // if it exists, we output the result, if not, then we do not output. 
    			  context.write(new Text(file_and_tfidf[0]), new DoubleWritable(Double.valueOf(file_and_tfidf[1])));    			  
    		  }// end of if

    	  }// end of for
         }
      }
   
   // the input of to the reduce class is basically the file name witht he tf_idf list associated witht he file for the query
   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text file_name,  Iterable<DoubleWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         double sum  = 0;
         // We iterate throught the counts iterable, adding up the value for each file and then returning them. 
         for ( DoubleWritable count  : counts) {
            sum  += count.get();
         }
         
         context.write(file_name,  new DoubleWritable(sum));
      }
   }
}
