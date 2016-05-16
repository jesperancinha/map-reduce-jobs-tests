package com.steelzack.mr.string.reformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by joao on 16-5-16.
 */
public class StringReFormat {

    public static class ReduceAllMessagesToFile extends Reducer<Text, Text, Text, Text> {


        public void reduce(Text key, Iterable<Text> values, Reducer.Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                context.write(key, value);
            }

        }
    }

    public static class MapMessagesToSend extends Mapper<Text, Text, Text, Text> {

        private BufferedReader reader;

        @Override
        public void map(Text key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {

            final String inKey = key.toString();
            if (inKey.length() != 0) {
                HashMap<String, String> linedoc = new HashMap<String, String>();
                reader = new BufferedReader(new FileReader(new File("/tmp/input.txt")));
                String pattern;
                while ((pattern = reader.readLine()) != null) {
                    if (pattern.length() == 0) {
                        break;
                    }
                    String[] line = pattern.split(",");

                    final String currentKey = line[0].trim();
                    if (!inKey.equals(currentKey)) {
                        linedoc.put(currentKey, line[1]);
                    }
                }

                for (String dpc : linedoc.keySet()) {
                    String pair = "(" + key + "," + dpc + ")";
                    context.write(new Text(pair), new Text(value.toString()));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "string reformat");
        job.setJarByClass(StringReFormat.class);
        job.setMapperClass(MapMessagesToSend.class);
        job.setCombinerClass(ReduceAllMessagesToFile.class);
        job.setReducerClass(ReduceAllMessagesToFile.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Writable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
