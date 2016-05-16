package com.steelzack.mr.string.reformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.util.Arrays;

/**
 * Created by joao on 16-5-16.
 */
public class StringReFormatTest {

    @Test
    public void main() throws Exception {

        final InputStream is = getClass().getResourceAsStream("/input.txt");
        try (final BufferedReader bis = new BufferedReader(new InputStreamReader(is));
             BufferedWriter bw = new BufferedWriter(new FileWriter("/tmp/input.txt"))) {
            String s;
            while ((s = bis.readLine()) != null) {
                bw.write(s);
                bw.write("\n");
            }
        }

        final Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        final Job job = Job.getInstance(conf, "string reformat");
        job.setMapperClass(StringReFormat.MapMessagesToSend.class);
        job.setReducerClass(StringReFormat.ReduceAllMessagesToFile.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("/tmp/input.txt"));
        final String ouputPathString = "/tmp/output" + LocalDateTime.now().getNano();
        FileOutputFormat.setOutputPath(job, new Path(ouputPathString));
        job.waitForCompletion(true);
        final File directory = new File(ouputPathString);
        Arrays.stream(directory.listFiles()).forEach(File::delete);
        directory.delete();

    }

}
