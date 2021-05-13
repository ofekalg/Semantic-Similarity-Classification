package Jobs;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class JFS_calc_vector1 {
    public static class MapperClassJFS_calc_vector1 extends Mapper<LongWritable, Text, Text, Text> {
        private Set<String> gold_vocabulary;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();

            gold_vocabulary = new HashSet<>();
            String[] words = conf.get("gold_vocabulary").split(", ");
            Collections.addAll(gold_vocabulary, words);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) {
            String[] record = value.toString().split(" ");
            int amount = Integer.parseInt(record[record.length - 1]);

            for (int i = 0; i < record.length - 1; i++) {
                try {
                    String[] ngram = record[i].split("/");

                    int key_index;
                    if (ngram.length > 4) // Slash is a word
                        key_index = Integer.parseInt(ngram[4]) - 1;
                    else
                        key_index = Integer.parseInt(ngram[3]) - 1;
                    if (key_index < 0)
                        continue;

                    Text feature = new Text(ngram[0] + " " + ngram[2] + " " + amount);

                    String word = record[key_index].split("/")[0];
                    if (!gold_vocabulary.contains(word))
                        continue;

                    context.write(new Text(word), feature);
                }
                catch (Exception e) {
                    System.err.println(e.getMessage());
                }
            }
        }
    }

    public static class CombinerClassJFS_calc_vector1 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> already_seen = new HashMap<>();

            for (Text val : values) {
                String[] ngram = val.toString().split(" ");
                String mapKey = ngram[0] + " " + ngram[1];
                int mapValue = Integer.parseInt(ngram[2]);

                if (already_seen.containsKey(mapKey)) {
                    int newValue = already_seen.get(mapKey) + mapValue;
                    already_seen.replace(mapKey, newValue);
                }
                else {
                    already_seen.put(mapKey, mapValue);
                }
            }

            for (String mapKey : already_seen.keySet()) {
                String feature = mapKey + " " + already_seen.get(mapKey);
                context.write(key, new Text(feature));
            }
        }
    }

    public static class ReducerClassJFS_calc_vector1 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder vector = new StringBuilder();
            Map<String, Integer> already_seen = new HashMap<>();

            for (Text val : values) {
                String[] ngram = val.toString().split(" ");
                String mapKey = ngram[0] + " " + ngram[1];
                int mapValue = Integer.parseInt(ngram[2]);

                if (already_seen.containsKey(mapKey)) {
                    int newValue = already_seen.get(mapKey) + mapValue;
                    already_seen.replace(mapKey, newValue);
                }
                else {
                    already_seen.put(mapKey, mapValue);
                }
            }

            for (String mapKey : already_seen.keySet()) {
                String feature = mapKey + " " + already_seen.get(mapKey);
                vector.append(feature).append("\t");
            }

            context.write(key, new Text(vector.substring(0, vector.length() - 1)));
        }
    }

    public static class MapperClassJFS_calc_vector1_2 extends Mapper<Text, Text, Text, Text> {
        private Set<String> gold_vocabulary;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();

            gold_vocabulary = new HashSet<>();
            String[] words = conf.get("gold_vocabulary").split(", ");
            Collections.addAll(gold_vocabulary, words);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("dsp211")) {
                for (String wordKey : gold_vocabulary)
                    context.write(new Text(wordKey), new Text("0\t" + value));
            }
            else {
                context.write(key, new Text("1\t" + value));
            }
        }
    }

    public static class ReducerClassJFS_calc_vector1_2 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder vector = new StringBuilder();
            Map<String, Integer> features = new LinkedHashMap<>();

            Map<String, Integer> vector_amounts = new LinkedHashMap<>();
            for (Text val : values) {
                String[] val_split = val.toString().split("\\t+");
                if (val_split[0].equals("0")) {
                    for (String x : Arrays.copyOfRange(val_split, 1, val_split.length))
                        features.put(x, 0);
                }
                else {
                    for (String x : Arrays.copyOfRange(val_split, 1, val_split.length)) {
                        String[] x_split = x.split(" ");
                        vector_amounts.put(x_split[0] + " " + x_split[1], Integer.parseInt(x_split[2]));
                    }
                }
            }

            for (String v_key : vector_amounts.keySet()) {
                if (features.containsKey(v_key))
                    features.put(v_key, vector_amounts.get(v_key));
            }

            for (String mapKey : features.keySet()) {
                String amount = Integer.toString(features.get(mapKey));
                vector.append(amount).append("\t");
            }

            context.write(key, new Text(vector.substring(0, vector.length() - 1)));
        }
    }

    // Reading the file from s3 and turning it into a string
    private static String getVocab() {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();

        S3Object getObjectResponse = s3.getObject("ass3-corpus-bucket", "Gold-vocabulary/part-r-00000");
        BufferedReader reader = new BufferedReader(new InputStreamReader(getObjectResponse.getObjectContent()));

        StringBuilder vocab = new StringBuilder();
        String line;

        try {
            while ((line = reader.readLine()) != null) {
                vocab.append(line.split("\\t+")[0]).append(", ");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        // Removing the last ", "
        return vocab.substring(0, vocab.length() - 2);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("gold_vocabulary", getVocab());

        // ---------------------------------- Job1 ---------------------------------- //
        // Words and the features connected to them according to the corpus //
        Job job1 = Job.getInstance(conf, "JFS_calc_vectors1_1");
        job1.setJarByClass(JFS_calc_vector1.class);
        job1.setMapperClass(JFS_calc_vector1.MapperClassJFS_calc_vector1.class);
        job1.setReducerClass(JFS_calc_vector1.ReducerClassJFS_calc_vector1.class);
        job1.setCombinerClass(JFS_calc_vector1.CombinerClassJFS_calc_vector1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setNumReduceTasks(50);

        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        // ---------------------------------- Job2 ---------------------------------- //
        // --------------------------------- Vector1 -------------------------------- //
        Job job2 = Job.getInstance(conf, "JFS_calc_vectors1_2");
        job2.setJarByClass(JFS_calc_vector1.class);
        job2.setMapperClass(JFS_calc_vector1.MapperClassJFS_calc_vector1_2.class);
        job2.setReducerClass(JFS_calc_vector1.ReducerClassJFS_calc_vector1_2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setNumReduceTasks(50);

        FileInputFormat.addInputPath(job2, new Path(args[2] + "*"));
        FileInputFormat.addInputPath(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
