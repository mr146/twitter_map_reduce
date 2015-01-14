package twittercounter;

import java.io.IOException;
import java.util.*;
         
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class TwitterAggregatorReducer
    extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
  
    public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) 
         throws IOException, InterruptedException {
           int sum = 0;
           for (IntWritable val : values) {
               sum += val.get();
           }
           context.write(key, new IntWritable(sum));
    }
}
