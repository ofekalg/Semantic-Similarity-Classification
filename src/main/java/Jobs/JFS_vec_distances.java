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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

public class JFS_vec_distances {
    public static class MapperClassJFS_vec_distances extends Mapper<Text, Text, Text, Text> {
        Map<String, LinkedList<String>> word_relatedness;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();

            word_relatedness = new LinkedHashMap<>();
            String[] pairs = conf.get("word_relatedness").split(", ");

            for (String p : pairs) {
                String[] words = p.split("\\t+");
                String word1 = words[0];
                String word2 = words[1];

                LinkedList<String> value;
                if (word_relatedness.containsKey(word1))
                    value = word_relatedness.get(word1);
                else
                    value = new LinkedList<>();

                value.add(word2);
                word_relatedness.put(word1, value);
            }
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String key_str = key.toString();

            if (word_relatedness.containsKey(key_str)) {
                LinkedList<String> relatives = word_relatedness.get(key_str);
                for (String relative : relatives) {
                    String new_key = key_str + "\t" + relative;
                    context.write(new Text(new_key), value);
                }
            }

            for (String map_key : word_relatedness.keySet()) {
                LinkedList<String> relatives = word_relatedness.get(map_key);
                if (relatives.contains(key_str)) {
                    String new_key = map_key + "\t" + key_str;
                    context.write(new Text(new_key), value);
                }
            }
        }
    }

    public static class ReducerClassJFS_vec_distances extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double distance1 = 0.0;
            double distance2 = 0.0;

            double numerator_dist3 = 0.0;
            double denominator1_dist3 = 0.0;
            double denominator2_dist3 = 0.0;

            double numerator_dist4_5 = 0.0;
            double denominator_dist4 = 0.0;

            double denominator_dist5 = 0.0;

            double part1 = 0.0;
            double part2 = 0.0;

            String[] two_vectors = new String[2];
            int index = 0;
            for (Text val : values) {
                two_vectors[index] = val.toString();
                index++;
            }

            String[] vector1 = two_vectors[0].split("\\t+");
            String[] vector2 = two_vectors[1].split("\\t+");
            for (int i = 0; i < 1000; i++) {
                double val1 = Double.parseDouble(vector1[i]);
                double val2 = Double.parseDouble(vector2[i]);
                distance1 += Math.abs(val1 - val2);
                distance2 += Math.pow(val1 - val2, 2);

                numerator_dist3 += (val1 * val2);
                denominator1_dist3 += Math.pow(val1, 2);
                denominator2_dist3 += Math.pow(val2, 2);

                numerator_dist4_5 += Math.min(val1, val2);
                denominator_dist4 += Math.max(val1, val2);

                denominator_dist5 += (val1 + val2);

                double middle = (val1 + val2) / 2;
                if (middle > 0.0) {
                    if (val1 > 0.0)
                        part1 += (val1 * Math.log(val1 / middle));

                    if (val2 > 0.0)
                        part2 += (val2 * Math.log(val2 / middle));
                }
            }

            distance2 = Math.sqrt(distance2);

            double distance3;
            if (denominator1_dist3 != 0.0 && denominator2_dist3 != 0.0)
                distance3 = numerator_dist3 / (Math.sqrt(denominator1_dist3) * Math.sqrt(denominator2_dist3));
            else
                distance3 = 1.0;

            double distance4;
            if (denominator_dist4 != 0.0)
                distance4 = numerator_dist4_5 / denominator_dist4;
            else
                distance4 = 1.0;

            double distance5;
            if (denominator_dist5 != 0.0)
                distance5 = 2 * numerator_dist4_5 / denominator_dist5;
            else
                distance5 = 2.0;

            double distance6 = part1 + part2;

            int vector_index = context.getConfiguration().getInt("vector_index", 0);
            String vector = vector_index + "\t" + distance1 + "\t" + distance2 + "\t" +
                    distance3 + "\t" + distance4 + "\t" + distance5 + "\t" + distance6;

            context.write(key, new Text(vector));
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
                String[] line_split = line.split("\\t+");
                word_relatedness.append(line_split[0]).append("\t").append(line_split[1]).append(", ");
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
        conf.setInt("vector_index", Integer.parseInt(args[3]));

        // ---------------------------------- Job1 ---------------------------------- //
        // --------------------------- Calculate distances -------------------------- //
        Job job = Job.getInstance(conf, "JFS_vec_distances_all");
        job.setJarByClass(JFS_vec_distances.class);
        job.setMapperClass(JFS_vec_distances.MapperClassJFS_vec_distances.class);
        job.setReducerClass(JFS_vec_distances.ReducerClassJFS_vec_distances.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(50);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
