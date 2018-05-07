package com.bluerain.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


public class SortByIntValueReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  private IntWritable result = new IntWritable();
  private Text word = new Text();

  private Integer limit = null;
  private Map<String, Integer> wordCountMap = new HashMap();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    if (conf.getBoolean("wordcount.hasLimit", false)) {
      limit = conf.getInt("wordcount.limit", Integer.MAX_VALUE);
    }
  }

  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
                     Context context) throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    //result.set(sum);
    //context.write(key, result);
    wordCountMap.put(key.toString(), sum);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    Map<String, Integer> sortedMap = wordCountMap.entrySet()
        .stream()
        .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
        .limit(limit)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
            (oldValue, newValue) -> oldValue, LinkedHashMap::new));

    for (Map.Entry<String, Integer> e : sortedMap.entrySet()) {
      word.set(e.getKey());
      result.set(e.getValue().intValue());
      context.write(word, result);
    }
  }

  private void save(Context context, String key, Integer count) throws IOException, InterruptedException {
    word.set(key);
    result.set(count.intValue());
    context.write(word, result);
  }
}

