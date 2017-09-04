// Source: Tom White, Hadoop Book
// https://github.com/tomwhite/hadoop-book/blob/master/ch08-mr-types/src/main/java/WholeFileInputFormat.java
// cc WholeFileInputFormat An InputFormat for reading a whole file as a record
import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

//vv WholeFileInputFormat
public class WholeFileInputFormat
    extends FileInputFormat<NullWritable, BytesWritable> {
  
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }

  @Override
  public RecordReader<NullWritable, BytesWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    WholeFileRecordReader reader = new WholeFileRecordReader();
    reader.initialize(split, context);
    return reader;
  }
}
//^^ WholeFileInputFormat