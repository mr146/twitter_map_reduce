package twittercounter;

import java.io.IOException;
import java.util.*;
         
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class TwitterGetRangesMapper
    extends Mapper<Text, Text, IntWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private int getDegree(int x)
    {
        int deg = 0;
        int val = 1;
        while(val * 10 <= x)
        {
            val *= 10;
            deg++;
        }
        return deg;
    }

    @Override
    public void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
        long userId = Long.parseLong(key.toString(), 10);
        int count = Integer.parseInt(value.toString(), 10);
        int radix = getDegree(count);
        context.write(new IntWritable(radix), one);
    }
}
