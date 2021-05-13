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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class JFS_calc_vector3 {
    enum Count_L {VALUE};

    public static class MapperClassJFS_calc_vectors3_count_L extends Mapper<Text, Text, Text, LongWritable> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("Count_L"), new LongWritable(Long.parseLong(value.toString())));
        }
    }

    public static class ReducerClassJFS_calc_vectors3_count_L extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }

            context.getCounter(Count_L.VALUE).setValue(sum);
            context.write(key, new LongWritable(sum));
        }
    }

    public static class MapperClassJFS_calc_vectors3_p_l_f extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            long count_L = context.getConfiguration().getLong("Count_L", 1);

            String[] val_split = value.toString().split("\\t+");
            StringBuilder vector = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                double prob = Double.parseDouble(val_split[i]) / count_L;
                vector.append(prob).append("\t");
            }

            context.write(key, new Text(vector.substring(0, vector.length() - 1)));
        }
    }

    public static class MapperClassJFS_calc_vectors3_p_l extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            long count_L = context.getConfiguration().getLong("Count_L", 1);
            double prob = Double.parseDouble(value.toString()) / count_L;

            context.write(key, new Text(Double.toString(prob)));
        }
    }

    public static class MapperClassJFS_calc_vectors3_p_f extends Mapper<Text, Text, Text, LongWritable> {
        String[] top1000;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();

            top1000 = conf.get("top1000").split(", ");
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] value_split = value.toString().split("\\t+");
            for (int i = 0; i < 1000; i++) {
                context.write(new Text(top1000[i]), new LongWritable(Long.parseLong(value_split[i])));
            }
        }
    }

    public static class ReducerClassJFS_calc_vectors3_p_f extends Reducer<Text, LongWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count_F = context.getConfiguration().getLong("Count_L", 1);

            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }

            double prob = sum / (double) (count_F);
            context.write(key, new Text(Double.toString(prob)));
        }
    }

    public static class MapperClassJFS_calc_vectors3_vec3 extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class ReducerClassJFS_calc_vectors3_vec3 extends Reducer<Text, Text, Text, Text> {
        String[] p_f;

        @Override
        public void setup(Reducer.Context context) {
            Configuration conf = context.getConfiguration();

            p_f = conf.get("p_f").split(", ");
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double p_l = 0.0;
            double[] new_values = new double[1000];

            for (Text val : values) {
                String[] val_split = val.toString().split("\\t+");
                if (val_split.length < 1000) {
                    p_l = Double.parseDouble(val_split[0]);
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
                if (p_l != 0 && Double.parseDouble(p_f[i]) != 0.0) {
                    double temp_value = new_values[i] / (p_l * Double.parseDouble(p_f[i]));
                    if (temp_value <= 0.0)
                        new_values[i] = 0.0;
                    else
                        new_values[i] = Math.log(temp_value) / Math.log(2);
                }
                else
                    new_values[i] = 0.0;
            }

            StringBuilder vector = new StringBuilder();
            for (int i = 0; i < 1000; i++)
                vector.append(new_values[i]).append("\t");

            context.write(key, new Text(vector.substring(0, vector.length() - 1)));
        }
    }

    public static class MapperClassJFS_calc_vectors3_vec4 extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class ReducerClassJFS_calc_vectors3_vec4 extends Reducer<Text, Text, Text, Text> {
        String[] p_f;

        @Override
        public void setup(Reducer.Context context) {
            Configuration conf = context.getConfiguration();

            p_f = conf.get("p_f").split(", ");
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double p_l = 0.0;
            double[] new_values = new double[1000];

            for (Text val : values) {
                String[] val_split = val.toString().split("\\t+");
                if (val_split.length < 1000) {
                    p_l = Double.parseDouble(val_split[0]);
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
                if (p_l != 0 && Double.parseDouble(p_f[i]) != 0.0) {
                    double p_f_val = Double.parseDouble(p_f[i]);
                    double p_lxp_f = p_l * p_f_val;
                    double temp_value = (new_values[i] - p_lxp_f) / Math.sqrt(p_lxp_f);
                    new_values[i] = temp_value;
                }
                else
                    new_values[i] = 1.0;
            }

            StringBuilder vector = new StringBuilder();
            for (int i = 0; i < 1000; i++)
                vector.append(new_values[i]).append("\t");

            context.write(key, new Text(vector.substring(0, vector.length() - 1)));
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

    // Reading the file from s3 and turning it into a string
    private static String getP_f() {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();

        S3Object getObjectResponse = s3.getObject("ass3-small-output-bucket", "P_f/part-r-00000");
        BufferedReader reader = new BufferedReader(new InputStreamReader(getObjectResponse.getObjectContent()));

        StringBuilder p_f = new StringBuilder();
        String line;

        try {
            while ((line = reader.readLine()) != null) {
                p_f.append(line.split("\\t+")[1]).append(", ");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        // Removing the last ", "
        return p_f.substring(0, p_f.length() - 2);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("top1000", getTop1000());

        // ---------------------------------- Job1 ---------------------------------- //
        // --------------------------- Calculate count(L) --------------------------- //
        Job job1 = Job.getInstance(conf, "JFS_calc_vectors3_1");
        job1.setJarByClass(JFS_calc_vector3.class);
        job1.setMapperClass(JFS_calc_vector3.MapperClassJFS_calc_vectors3_count_L.class);
        job1.setReducerClass(JFS_calc_vector3.ReducerClassJFS_calc_vectors3_count_L.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        // ---------------------------------- Job2 ---------------------------------- //
        // --------------------------- Calculate P(l, f) ---------------------------- //
        long count_L = job1.getCounters().findCounter(Count_L.VALUE).getValue();
        conf.setLong("Count_L", count_L);

        Job job2 = Job.getInstance(conf, "JFS_calc_vectors3_2");
        job2.setJarByClass(JFS_calc_vector3.class);
        job2.setMapperClass(JFS_calc_vector3.MapperClassJFS_calc_vectors3_p_l_f.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
        job2.waitForCompletion(true);

        // ---------------------------------- Job3 ---------------------------------- //
        // ----------------------------- Calculate P(l) ----------------------------- //
        Job job3 = Job.getInstance(conf, "JFS_calc_vectors3_3");
        job3.setJarByClass(JFS_calc_vector3.class);
        job3.setMapperClass(JFS_calc_vector3.MapperClassJFS_calc_vectors3_p_l.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        job3.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job3, new Path(args[1]));
        FileOutputFormat.setOutputPath(job3, new Path(args[5]));
        job3.waitForCompletion(true);

        // ---------------------------------- Job4 ---------------------------------- //
        // ----------------------------- Calculate P(f) ----------------------------- //
        Job job4 = Job.getInstance(conf, "JFS_calc_vectors3_4");
        job4.setJarByClass(JFS_calc_vector3.class);
        job4.setMapperClass(JFS_calc_vector3.MapperClassJFS_calc_vectors3_p_f.class);
        job4.setReducerClass(JFS_calc_vector3.ReducerClassJFS_calc_vectors3_p_f.class);

        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(LongWritable.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        job4.setInputFormatClass(SequenceFileInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        job4.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job4, new Path(args[3]));
        FileOutputFormat.setOutputPath(job4, new Path(args[6]));
        job4.waitForCompletion(true);

        // ---------------------------------- Job5 ---------------------------------- //
        // --------------------------------- Vector3 -------------------------------- //
        conf.set("p_f", getP_f());

        Job job5 = Job.getInstance(conf, "JFS_calc_vectors3_5");
        job5.setJarByClass(JFS_calc_vector3.class);
        job5.setMapperClass(JFS_calc_vector3.MapperClassJFS_calc_vectors3_vec3.class);
        job5.setReducerClass(JFS_calc_vector3.ReducerClassJFS_calc_vectors3_vec3.class);

        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        job5.setInputFormatClass(SequenceFileInputFormat.class);
        job5.setOutputFormatClass(SequenceFileOutputFormat.class);
        job5.setNumReduceTasks(50);

        FileInputFormat.addInputPath(job5, new Path(args[4] + "*"));
        FileInputFormat.addInputPath(job5, new Path(args[5] + "*"));
        FileOutputFormat.setOutputPath(job5, new Path(args[7]));
        job5.waitForCompletion(true);

        // ---------------------------------- Job6 ---------------------------------- //
        // --------------------------------- Vector4 -------------------------------- //
        Job job6 = Job.getInstance(conf, "JFS_calc_vectors3_6");
        job6.setJarByClass(JFS_calc_vector3.class);
        job6.setMapperClass(JFS_calc_vector3.MapperClassJFS_calc_vectors3_vec4.class);
        job6.setReducerClass(JFS_calc_vector3.ReducerClassJFS_calc_vectors3_vec4.class);

        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(Text.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);

        job6.setInputFormatClass(SequenceFileInputFormat.class);
        job6.setOutputFormatClass(SequenceFileOutputFormat.class);
        job6.setNumReduceTasks(50);

        FileInputFormat.addInputPath(job6, new Path(args[4] + "*"));
        FileInputFormat.addInputPath(job6, new Path(args[5] + "*"));
        FileOutputFormat.setOutputPath(job6, new Path(args[8]));
        System.exit(job6.waitForCompletion(true) ? 0 : 1);
    }
}
