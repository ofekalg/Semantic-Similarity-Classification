package Others;

import java.io.IOException;

import Utils.Stemmer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class ReadCorpus {
    public static class MapperClass extends Mapper<LongWritable, Text, LongWritable, Text> {
        Stemmer s = new Stemmer();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split("\\t+");
            String[] syntactic_ngrams = record[1].split(" ");

            String newValue = "";
            for (String s_ngram : syntactic_ngrams) {
                String[] ngram = s_ngram.split("/");
                ngram[0] = s.stem(ngram[0]);
                newValue += ngram[0] + "/" + ngram[1] + "/" +ngram[2] + "/" +ngram[3] + " ";
            }
            newValue += record[2];
            context.write(key, new Text(newValue));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Corpus reader");
        job.setJarByClass(ReadCorpus.class);
        job.setMapperClass(MapperClass.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
