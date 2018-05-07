package com.bluerain.hadoop.mapred;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.List;

/**
 * Using Apache Hadoop MapReduce to analyze a text file. Return a list of unique
 * words and their count in the descending order.
 */
public class WordCountApp {
  @Parameter(
      names = "-n",
      description = "Number of most frequently used words in text file."
  )
  public Integer limit;  // default is to return all

  @Parameter(
      names = {"-i", "-ignoreCase"},
      description = "Ignore case. Default is case-sensitive word count."
  )
  public boolean ignoreCase = false;

  @Parameter(
      names = {"-s", "-skip"},
      required = true,
      description = "Specify file the full path name to the stop words file."

  )
  public String stopWordsFile;

  @Parameter(arity = 2, description = "Input and Output files")
  public List<String> files;


  public static void main(String[] args) throws Exception {
    WordCountApp app = new WordCountApp();
    JCommander.newBuilder()
        .addObject(app)
        .build()
        .parse(args);
    System.out.println(String.format("Returns %s words from the input files and their count. Words are read %s.",
        (app.limit != null) ? "top " + app.limit : "all",
        app.ignoreCase ? "case-insensitive" : "case-sensitive"));

    // configure Hadoop Jobs
    Configuration conf = new Configuration();
    conf.setBoolean("wordcount.ignoreCase", app.ignoreCase);
    conf.setBoolean("wordcount.hasLimit", app.limit != null && app.limit > 0);
    if (app.limit != null && app.limit > 0) {
      conf.setBoolean("wordcount.hasLimit", true);
      conf.setInt("wordcount.limit", app.limit);
    }
    Job job = Job.getInstance(conf, "word count");
    if (StringUtils.isNoneEmpty(app.stopWordsFile)) {
      job.getConfiguration().setBoolean("wordcount.hasStopWords", true);
      job.addCacheFile(new Path(app.stopWordsFile).toUri());
    }
    job.setJarByClass(WordCountApp.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(SortByIntValueReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(app.files.get(0)));
    FileOutputFormat.setOutputPath(job, new Path(app.files.get(1)));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
