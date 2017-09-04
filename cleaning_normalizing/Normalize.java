// This reads each file in as a whole record and normalizes the column
// of each datapoint to have a value between 0 and 1 using the following
// formula
//  z = (x - min(x)) / (max(x) - min(x))
//  Analysis from NormalizeMaxMinVals.java showed that the max value for each
//  column was 255 and the min value is 0
//  This is unsurprising given that the examples correspond to a diverse set of images
//  and fortunately makes the normalization task simpler
//  Output from the reducer is one file per class. Each file is a sequence of bytes
//  One record is label + data. Data consists of 4 * 3072 bytes

import java.io.IOException;
import java.nio.ByteBuffer;
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


public class Normalize {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: Normalize <inputPath> <outputPath>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(Normalize.class);
    job.setJobName("Normalize by Laura G");
    job.setNumReduceTasks(10);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setInputFormatClass(WholeFileInputFormat.class);
 
    job.setMapperClass(NormalizeMapper.class);
    job.setReducerClass(NormalizeReducer.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

class NormalizeMapper extends Mapper<NullWritable, BytesWritable, IntWritable, BytesWritable> {

  @Override
  public void map(NullWritable key, BytesWritable value, Context context) 
    throws IOException, InterruptedException {

    byte[] file = new byte[30730000];
    file = value.copyBytes();
    int record_length = 3073;
    int num_records = 10000;
    float max_val = 255.0f;
    float min_val = 0.0f;

    // Normalize all of the pixels and rebuild each example
    for (int j = 0; j < num_records; j++) {
      int curr_label = -1;
      byte[] record = new byte[3072 * 4];
      for (int i = 0; i < record_length; i++ ) {
        int idx = j * record_length + i;
        Byte b = file[idx];
        if (i == 0) {
          curr_label = b.intValue();
        }
        else {
          int newidxstart = (i - 1) * 4;
          float curr_val = b.floatValue() + 128;  // Bytes are signed, pixel vals are 0 - 255
          float norm_val = (curr_val - min_val) / (max_val - min_val);
          // Convert to bytes
          byte[] bb = ByteBuffer.allocate(4).putFloat(norm_val).array();
          for (int k = 0; k < bb.length; k++) {
            record[newidxstart + k] = bb[k];
          }
        }
      }
      context.write(new IntWritable(curr_label), new BytesWritable(record));
    }
  }
}

class NormalizeReducer extends Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {

  @Override
  public void reduce(IntWritable key, Iterable<BytesWritable> values, Context context)
    throws IOException, InterruptedException {
    
    for (BytesWritable value : values) {
    context.write(key, value);
    }
  }
} 