// This reads each file in as a whole record and counts the number of images from each class

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


public class ProfileCIFARData2 {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: ProfileCIFARData2 <inputPath> <outputPath>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(ProfileCIFARData2.class);
    job.setJobName("ProfileCIFARData2 by Laura G");
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setInputFormatClass(WholeFileInputFormat.class);
 
    job.setMapperClass(ProfileCIFARData2Mapper.class);
    job.setReducerClass(ProfileCIFARData2Reducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

class ProfileCIFARData2Mapper extends Mapper<NullWritable, BytesWritable, Text, Text> {

  @Override
  public void map(NullWritable key, BytesWritable value, Context context) 
    throws IOException, InterruptedException {

    byte[] file = new byte[30730000];
    Byte curr_label;
    int curr_label_int;
    file = value.copyBytes();
    int record_length = 3073;
    int idx = 0;
    while (idx < 30730000) {
      curr_label = file[idx];
      curr_label_int = curr_label.intValue();
      String label_s  = "";
      switch(curr_label_int) {
          case 0: label_s = "airplane"; break;
          case 1: label_s = "automobile"; break;
          case 2: label_s = "bird"; break;
          case 3: label_s = "cat"; break;
          case 4: label_s = "deer"; break;
          case 5: label_s = "dog"; break;
          case 6: label_s = "frog"; break;
          case 7: label_s = "horse"; break;
          case 8: label_s = "ship"; break;
          case 9: label_s = "truck"; break;
          default: label_s = "error"; break;
      }
      String out = "" + 1;
      context.write(new Text(label_s), new Text(out));
      idx += record_length;
      }
  }
}

class ProfileCIFARData2Reducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
    
    String label = key.toString();
    int num_images = 0;
    for (Text value : values) {
        String count = value.toString();
        int count_int = Integer.parseInt(count);
        num_images += count_int;
    }
    String result = "Actual num images: " + num_images + "    Correct num images: 5000 (training set), 1000 (test set)";
    context.write(new Text(label), new Text(result));
  }
} 