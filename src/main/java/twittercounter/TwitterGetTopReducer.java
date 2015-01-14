package twittercounter;

import java.io.IOException;
import java.util.*;
         
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class TwitterGetTopReducer
    extends Reducer<IntWritable, LongWritable, Text, Text> {

    public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) 
         throws IOException, InterruptedException {
           int count = -key.get();
           Text empty = new Text("");
           for (LongWritable userId : values) {
               context.write(new Text(String.format("%d has %d followers", userId.get(), count)), empty);
           }
    }
}
