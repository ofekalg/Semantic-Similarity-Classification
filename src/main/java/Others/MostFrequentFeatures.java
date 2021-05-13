package Others;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class MostFrequentFeatures {
    public static class MapperClassMostFrequentFeatures1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) {
            String[] record = value.toString().split(" ");
            int amount = Integer.parseInt(record[record.length - 1]);

            for (int i = 0; i < record.length - 1; i++) {
                try {
                    String[] ngram = record[i].split("/");

                    Text feature = new Text(ngram[0] + " " + ngram[2]);
                    context.write(feature, new IntWritable(amount));
                }
                catch (Exception e) {
                    System.err.println(e.getMessage());
                }
            }
        }
    }

    public static class ReducerClassMostFrequentFeatures1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static class MapperClassMostFrequentFeatures2 extends Mapper<Text, IntWritable, IntWritable, Text> {
        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(value, key);
        }
    }

    public static class ReducerClassMostFrequentFeatures2 extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder record = new StringBuilder();

            for (Text val : values) {
                record.append(val).append("\t");
            }

            context.write(key, new Text(record.substring(0, record.length() - 1)));
        }
    }

    public static class MapperClassMostFrequentFeatures3 extends Mapper<LongWritable, Text, Text, Text> {
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            Set<String> top1000 = new HashSet<>();
            String[] words = conf.get("top1000").split(", ");
            Collections.addAll(top1000, words);

            for (String feature : top1000)
                context.write(new Text("dsp211"), new Text(feature));
        }

        public void map(LongWritable key, Text value, Context context) {
        }
    }

    public static class ReducerClassMostFrequentFeatures3 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder record = new StringBuilder();

            for (Text val : values) {
                record.append(val).append("\t");
            }

            context.write(key, new Text(record.substring(0, record.length() - 1)));
        }
    }

    // Reading the file from s3 and turning it into a string
    private static String getTop1000() {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();

        S3Object getObjectResponse = s3.getObject("ass3-corpus-bucket", "top1000.txt");
        BufferedReader reader = new BufferedReader(new InputStreamReader(getObjectResponse.getObjectContent()));

        StringBuilder top1000 = new StringBuilder();
        String line;

        try {
            while ((line = reader.readLine()) != null) {
                top1000.append(line.split("\\t+")[1]).append(", ");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        // Removing the last ", "
        return top1000.substring(0, top1000.length() - 2);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("top1000", getTop1000());

        // ---------------------------------- Job1 ---------------------------------- //
//        Job job1 = Job.getInstance(conf, "MostFrequentFeatures1");
//        job1.setJarByClass(MostFrequentFeatures.class);
//        job1.setMapperClass(MostFrequentFeatures.MapperClassMostFrequentFeatures1.class);
//        job1.setReducerClass(MostFrequentFeatures.ReducerClassMostFrequentFeatures1.class);
//
//        job1.setMapOutputKeyClass(Text.class);
//        job1.setMapOutputValueClass(IntWritable.class);
//        job1.setOutputKeyClass(Text.class);
//        job1.setOutputValueClass(IntWritable.class);
//
//        job1.setInputFormatClass(SequenceFileInputFormat.class);
//        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
//        job1.setNumReduceTasks(50);
//
//        FileInputFormat.addInputPath(job1, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
//        job1.waitForCompletion(true);
//
//        // ---------------------------------- Job2 ---------------------------------- //
//        Job job2 = Job.getInstance(conf, "MostFrequentFeatures2");
//        job2.setJarByClass(MostFrequentFeatures.class);
//        job2.setMapperClass(MostFrequentFeatures.MapperClassMostFrequentFeatures2.class);
//        job2.setReducerClass(MostFrequentFeatures.ReducerClassMostFrequentFeatures2.class);
//
//        job2.setMapOutputKeyClass(IntWritable.class);
//        job2.setMapOutputValueClass(Text.class);
//        job2.setOutputKeyClass(IntWritable.class);
//        job2.setOutputValueClass(Text.class);
//
//        job2.setInputFormatClass(SequenceFileInputFormat.class);
//        job2.setOutputFormatClass(TextOutputFormat.class);
//        job2.setNumReduceTasks(1);
//
//        FileInputFormat.addInputPath(job2, new Path(args[2] + "*"));
//        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
//        job2.waitForCompletion(true);

        // ---------------------------------- Job3 ---------------------------------- //
        Job job3 = Job.getInstance(conf, "MostFrequentFeatures3");
        job3.setJarByClass(MostFrequentFeatures.class);
        job3.setMapperClass(MostFrequentFeatures.MapperClassMostFrequentFeatures3.class);
        job3.setReducerClass(MostFrequentFeatures.ReducerClassMostFrequentFeatures3.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        job3.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job3, new Path(args[3] + "*"));
        FileOutputFormat.setOutputPath(job3, new Path(args[4]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
