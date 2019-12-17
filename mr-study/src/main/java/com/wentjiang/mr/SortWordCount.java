package com.wentjiang.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @author wentao.jiang
 * @date 2019/12/16 10:25 AM
 * @description
 */
public class SortWordCount {

    private static String TOPK_KEY = "topK";
    private static int TOPK_VALUE = 10;

    public static class Pair<KEY, VALUE> {
        private KEY key;
        private VALUE value;

        public Pair(KEY key, VALUE value) {
            this.key = key;
            this.value = value;
        }

        public KEY getKey() {
            return key;
        }

        public VALUE getValue() {
            return value;
        }
    }

    public static class SortWordMapper extends Mapper<Object, Text, Text, IntWritable> {

        private List<Pair<String, Integer>> list;
        private int topK;

        @Override
        protected void setup(Context context) {
            list = new ArrayList<>();
            topK = context.getConfiguration().getInt(TOPK_KEY, 1);
        }

        @Override
        protected void map(Object key, Text value, Context context) {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n", false);
            while (itr.hasMoreTokens()) {
                String[] strs = itr.nextToken().split("\t");
                String keyStr = strs[0];
                String valueStr = strs[1];
                list.add(new Pair<>(keyStr, Integer.valueOf(valueStr)));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            list.sort((o1, o2) -> o2.getValue() - o1.getValue());
            int i = 0;
            for (Pair<String, Integer> pair : list) {
                i++;
                if (i > topK) {
                    break;
                }
                context.write(new Text(pair.getKey()), new IntWritable(pair.getValue()));
            }
        }
    }

    public static class SortWordReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private List<Pair<String, Integer>> list;
        private int topK;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            list = new ArrayList<>();
            topK = context.getConfiguration().getInt(TOPK_KEY, 1);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                list.add(new Pair<>(key.toString(), value.get()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            list.sort((o1, o2) -> o2.getValue() - o1.getValue());
            int i = 0;
            for (Pair<String, Integer> pair : list) {
                i++;
                if (i > topK) {
                    break;
                }
                context.write(new Text(pair.getKey()), new IntWritable(pair.getValue()));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt(TOPK_KEY, TOPK_VALUE);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "top k");
        job.setJarByClass(SortWordCount.class);
        job.setMapperClass(SortWordMapper.class);
        job.setCombinerClass(SortWordReduce.class);
        job.setReducerClass(SortWordReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
            new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
