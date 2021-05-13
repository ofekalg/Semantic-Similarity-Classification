package Jobs;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.Map;

public class JFS_vec_final {
    public static class MapperClassJFS_vec_final extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class ReducerClassJFS_vec_final extends Reducer<Text, Text, Text, Text> {
        Map<String, String> word_relatedness;

        @Override
        public void setup(Reducer.Context context) {
            Configuration conf = context.getConfiguration();

            word_relatedness = new LinkedHashMap<>();
            String[] couples = conf.get("word_relatedness").split(", ");

            for (String p : couples) {
                String[] words = p.split("\\t+");
                String pair = words[0] + "\t" + words[1];
                String relatedness = words[2];

                word_relatedness.put(pair, relatedness);
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] concat_distances = new String[4];

            for (Text val : values) {
                String[] val_split = val.toString().split("\\t+");
                StringBuilder distances = new StringBuilder();
                for (int i = 1; i < 7; i++) {
                    if (i == 6) {
                        distances.append(val_split[i]);
                        break;
                    }
                    distances.append(val_split[i]).append("\t");
                }
                concat_distances[Integer.parseInt(val_split[0]) - 1] = distances.toString();
            }

            StringBuilder vector = new StringBuilder();
            for (int i = 0; i < 4; i++)
                vector.append(concat_distances[i]).append("\t");

            String relatedness = word_relatedness.get(key.toString());

            // Removing the last "\t"
            context.write(new Text(vector.toString()), new Text(relatedness));
        }
    }

    // Reading the file from s3 and turning it into a string
    private static String getWordRelatedness() {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();

        S3Object getObjectResponse = s3.getObject("ass3-corpus-bucket", "Word-relatedness-stemmed/part-r-00000");
        BufferedReader reader = new BufferedReader(new InputStreamReader(getObjectResponse.getObjectContent()));

        StringBuilder word_relatedness = new StringBuilder();
        String line;

        try {
            while ((line = reader.readLine()) != null) {
                word_relatedness.append(line).append(", ");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        // Removing the last ", "
        return word_relatedness.substring(0, word_relatedness.length() - 2);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("word_relatedness", getWordRelatedness());

        // ---------------------------------- Job1 ---------------------------------- //
        // ------------------------------ Vectors final ----------------------------- //
        Job job1 = Job.getInstance(conf, "JFS_vec_final");
        job1.setJarByClass(JFS_vec_final.class);
        job1.setMapperClass(JFS_vec_final.MapperClassJFS_vec_final.class);
        job1.setReducerClass(JFS_vec_final.ReducerClassJFS_vec_final.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileInputFormat.addInputPath(job1, new Path(args[2]));
        FileInputFormat.addInputPath(job1, new Path(args[3]));
        FileInputFormat.addInputPath(job1, new Path(args[4]));
        FileOutputFormat.setOutputPath(job1, new Path(args[5]));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}
