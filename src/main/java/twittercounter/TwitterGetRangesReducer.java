package twittercounter;

import java.io.IOException;
import java.util.*;
         
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class TwitterGetRangesReducer
    extends Reducer<IntWritable, IntWritable, Text, Text> {

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
         throws IOException, InterruptedException {
           int degree = key.get();
           int val = 1;
           for(int i = 0; i < degree; i++)
               val *= 10;
           int counter = 0;
           for (IntWritable zzzz : values) {
               counter++;
           }
           String range = String.format("[%d..%d]: %d", val, val * 10, counter);
           context.write(new Text(range), new Text(""));
    }
}
