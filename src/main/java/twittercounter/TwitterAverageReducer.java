package twittercounter;

import java.io.IOException;
import java.util.*;
         
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class TwitterAverageReducer
    extends Reducer<IntWritable, IntWritable, Text, Text> {

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
         throws IOException, InterruptedException {
           int count = 0;
           long sum = 0;
           for (IntWritable val : values) {
               sum += val.get();
               count++;
           }
           String result = String.format("Users: %d, average followers: %.8f", count, (double)sum / count);
           context.write(new Text(result), new Text(""));
    }
}
