// This reads each file in as a whole record and checks that the number of bytes in each file is correct

import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class ProfileCIFARData1 {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: ProfileCIFARData1 <inputPath> <outputPath>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(ProfileCIFARData1.class);
    job.setJobName("ProfileCIFARData1 by Laura G");
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setInputFormatClass(WholeFileInputFormat.class);
 
    job.setMapperClass(ProfileCIFARData1Mapper.class);
    job.setReducerClass(ProfileCIFARData1Reducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

class ProfileCIFARData1Mapper extends Mapper<NullWritable, BytesWritable, Text, Text> {

  @Override
  public void map(NullWritable key, BytesWritable value, Context context) 
    throws IOException, InterruptedException {

    // Checking each file is the correct length
    int length = value.getLength();
    if (length == 30730000) {
        context.write(new Text("file"), new Text("ok" + "\t" + length));    
    }
    else {
        context.write(new Text("file"), new Text("not ok" + "\t" + length));
    }
  }
}

class ProfileCIFARData1Reducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
    
    String label = key.toString();
    int i = 0;
    for (Text value : values) {
        context.write(new Text(Integer.toString(i)), new Text(value));
        i++;
    }
  }
} 
