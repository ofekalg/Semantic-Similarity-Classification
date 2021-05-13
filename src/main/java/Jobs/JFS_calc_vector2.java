package Jobs;

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

import java.io.IOException;

public class JFS_calc_vector2 {
    public static class MapperClassJFS_calc_vectors2 extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] features = value.toString().split("\\t+");

            long sum = 0;
            for (String feature : features) {
                sum += Integer.parseInt(feature);
            }

            context.write(key, new Text(Long.toString(sum)));
        }
    }

    public static class MapperClassJFS_calc_vectors2_2 extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class ReducerClassJFS_calc_vectors2_2 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long count_l = 0;
            double[] new_values = new double[1000];

            for (Text val : values) {
                String[] val_split = val.toString().split("\\t+");
                if (val_split.length < 1000) {
                    count_l = Long.parseLong(val_split[0]);
                }
                else {
                    int index = 0;
                    for (String vec : val_split) {
                        new_values[index] = Double.parseDouble(vec);
                        index++;
                    }
                }
            }

            for (int i = 0; i < 1000; i++) {
                if (count_l != 0)
                    new_values[i] /= count_l;
                else
                    new_values[i] = 0;
            }

            StringBuilder vector = new StringBuilder();
            for (int i = 0; i < 1000; i++)
                vector.append(new_values[i]).append("\t");

            context.write(key, new Text(vector.substring(0, vector.length() - 1)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // ---------------------------------- Job1 ---------------------------------- //
        // --------------------------- Calculate count(l) --------------------------- //
        Job job1 = Job.getInstance(conf, "JFS_calc_vectors2_1");
        job1.setJarByClass(JFS_calc_vector2.class);
        job1.setMapperClass(JFS_calc_vector2.MapperClassJFS_calc_vectors2.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        // ---------------------------------- Job2 ---------------------------------- //
        // --------------------------------- Vector2 -------------------------------- //
        Job job2 = Job.getInstance(conf, "JFS_calc_vectors2_2");
        job2.setJarByClass(JFS_calc_vector2.class);
        job2.setMapperClass(JFS_calc_vector2.MapperClassJFS_calc_vectors2_2.class);
        job2.setReducerClass(JFS_calc_vector2.ReducerClassJFS_calc_vectors2_2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setNumReduceTasks(50);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileInputFormat.addInputPath(job2, new Path(args[2] + "*"));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
