// This reads each file in as a whole record and calculates the 
// max(x) and min(x) per column needed to normalize the training 
// and test dataset as follows
//  z = (x - min(x)) / (max(x) - min(x))
//  Output from the reducer is one line per column (3073):  
//        col   max(x)    min(x)

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


public class NormalizeMaxMinVals {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: NormalizeMaxMinVals <inputPath> <outputPath>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(NormalizeMaxMinVals.class);
    job.setJobName("NormalizeMaxMinVals by Laura G");
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setInputFormatClass(WholeFileInputFormat.class);
 
    job.setMapperClass(NormalizeMaxMinValsMapper.class);
    job.setReducerClass(NormalizeMaxMinValsReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

class NormalizeMaxMinValsMapper extends Mapper<NullWritable, BytesWritable, Text, Text> {

  @Override
  public void map(NullWritable key, BytesWritable value, Context context) 
    throws IOException, InterruptedException {

    byte[] file = new byte[30730000];
    file = value.copyBytes();
    int record_length = 3073;
    int num_records = 10000;
    // Arrays holding the max and min values observed 
    // Initialized to 0
    float[] col_max = new float[3072];
    float[] col_min = new float[3072];
    // Two arrays to keep count of the number of times the
    // max and min vals changed - for debugging purposes
    int[] max_num_changed = new int[3072];
    int[] min_num_changed = new int[3072];
    for (int i = 0; i < col_max.length; i++) {
      col_max[i] = 0;
      col_min[i] = 0;
      max_num_changed[i] = 0;
      min_num_changed[i] = 0;
    }

    // Gather max and min values per column
    // Plays same role as a Combiner but from within Mapper
    for (int j = 0; j < num_records; j++) {
      for (int i = 1; i < record_length; i++ ) {
        int idx = j * record_length + i;
        Byte curr_val_b = file[idx];
        float curr_val = curr_val_b.floatValue() + 128;  // Bytes are signed, pixel vals are 0 - 255
        if (curr_val > col_max[i - 1]) {
          col_max[i - 1] = curr_val;
          max_num_changed[i -1] ++;
        }
        if (curr_val < col_min[i - 1]) {
          col_min[i - 1] = curr_val;
          min_num_changed[i - 1] ++;
        }
      }
    }

    // Write max and min values to string
    for (int i = 0; i < col_max.length; i ++) {
      String num = "" + i;
      // Pad numbers
      if (num.length() == 1) {
        num = "000" + i;
      } else if (num.length() == 2) {
        num = "00" + i;
      } else if (num.length() == 3) {
        num = "0" + i;
      } else {
      }
      String label = "Col " + num;
      String val = "";
      val += Float.toString(col_max[i]) + "\t";
      val += Float.toString(col_min[i]) + "\t";
      val += Integer.toString(max_num_changed[i]) + "\t";
      val += Integer.toString(min_num_changed[i]) ;
      context.write(new Text(label), new Text(val));
    }
  }
}

class NormalizeMaxMinValsReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
    
    String label = key.toString();
    float max_val = 0;
    float min_val = 0;
    int max_changed = 0;
    int min_changed = 0;
    for (Text value : values) {
      String val = value.toString();
        String[] parts = val.split("\t");
        float curr_max = Float.parseFloat(parts[0]);
        float curr_min = Float.parseFloat(parts[1]);
        max_changed += Integer.parseInt(parts[2]);
        min_changed += Integer.parseInt(parts[3]);
        if (curr_max > max_val) {
          max_val = curr_max;
        }
        if (curr_min < min_val) {
          min_val = curr_min;
        }
    }

    String result = "";
    result += Float.toString(max_val) + "\t";
    result += Float.toString(min_val) + "\t";
    result += Integer.toString(max_changed) + "\t";
    result += Integer.toString(min_changed);
    context.write(new Text(label), new Text(result));
  }
} 