package twittercounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Runner extends Configured implements Tool {

  public Job buildAggregatingJob(String input) throws Exception{
      Configuration conf = new Configuration();
      Job job = new Job(conf, "aggregator");
      job.setJarByClass(getClass());
      job.setNumReduceTasks(1);

      job.setInputFormatClass(KeyValueTextInputFormat.class);

      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(TwitterAggregatorMapper.class);
      job.setCombinerClass(TwitterAggregatorReducer.class);
      job.setReducerClass(TwitterAggregatorReducer.class);

      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(input));
      FileOutputFormat.setOutputPath(job, new Path("twitter/temp"));
      return job;
  }
  
  public Job buildGetTopJob(String input) throws Exception{
      Configuration conf = new Configuration();
      Job job = new Job(conf, "gettop");
      job.setJarByClass(getClass());
      job.setNumReduceTasks(1);

      job.setInputFormatClass(KeyValueTextInputFormat.class);

      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(LongWritable.class);

      job.setMapperClass(TwitterGetTopMapper.class);
      job.setReducerClass(TwitterGetTopReducer.class);

      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(input));
      FileOutputFormat.setOutputPath(job, new Path("twitter/top"));
      return job;
  }
  
  public Job buildGetAverageJob(String input) throws Exception{
      Configuration conf = new Configuration();
      Job job = new Job(conf, "average");
      job.setJarByClass(getClass());
      job.setNumReduceTasks(1);

      job.setInputFormatClass(KeyValueTextInputFormat.class);

      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(TwitterAverageMapper.class);
      job.setReducerClass(TwitterAverageReducer.class);

      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(input));
      FileOutputFormat.setOutputPath(job, new Path("twitter/average"));
      return job;
  }

  public Job buildGetRangeJob(String input) throws Exception{
      Configuration conf = new Configuration();
      Job job = new Job(conf, "range");
      job.setJarByClass(getClass());
      job.setNumReduceTasks(1);

      job.setInputFormatClass(KeyValueTextInputFormat.class);

      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(TwitterGetRangesMapper.class);
      job.setReducerClass(TwitterGetRangesReducer.class);

      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(input));
      FileOutputFormat.setOutputPath(job, new Path("twitter/range"));
      return job;
  }

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    Job aggregatorJob = buildAggregatingJob(args[0]);
    Job getTopJob = buildGetTopJob("twitter/temp");
    Job getAverageJob = buildGetAverageJob("twitter/temp");
    Job getRangeJob = buildGetRangeJob("twitter/temp");

    aggregatorJob.waitForCompletion(true);
    getTopJob.waitForCompletion(true);
    getAverageJob.waitForCompletion(true);
    getRangeJob.waitForCompletion(true);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Runner(), args);
    System.exit(exitCode);
  }
}
