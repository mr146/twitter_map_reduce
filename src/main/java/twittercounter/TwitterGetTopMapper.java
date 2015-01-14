package twittercounter;

import java.io.IOException;
import java.util.*;
         
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class TwitterGetTopMapper
    extends Mapper<Text, Text, IntWritable, LongWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
        int count = Integer.parseInt(value.toString(), 10);
        long userId = Long.parseLong(key.toString(), 10);
        context.write(new IntWritable(-count), new LongWritable(userId));
    }
}
