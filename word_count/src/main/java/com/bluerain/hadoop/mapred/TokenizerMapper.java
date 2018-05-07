package com.bluerain.hadoop.mapred;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Using Apache Hadoop MapReduce to analyze a text file. Return a list of unique
 * words and their count in the descending order.
 */

public class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable> {

  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private boolean isIgnoreCase;
  private Set<String> stopWords = new HashSet<>();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    this.isIgnoreCase = conf.getBoolean("wordcount.ignoreCase", false);
    URI[] localFiles = context.getCacheFiles();
    for (URI file: localFiles) {
      try (Stream<String> lines = Files.lines(Paths.get(file))) {
        List<String> words = null;
        if (isIgnoreCase) {
          words = lines.map(String::toLowerCase).collect(Collectors.toList());
        } else {
          words = lines.collect(Collectors.toList());
        }
        stopWords.addAll(words);
      }
    }
  }

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    while (itr.hasMoreTokens()) {
      String token = isIgnoreCase ? itr.nextToken().toLowerCase() : itr.nextToken();
      if (StringUtils.isNoneEmpty(token) && !stopWords.contains(token)) {
        word.set(isIgnoreCase ? token.toLowerCase() : token);
        context.write(word, one);
      }
    }
  }
}

