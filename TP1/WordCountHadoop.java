import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Comptage Hadoop MapReduce avec le moteur LocalJobRunner. */
public final class WordCountHadoop {
    public static final class Tokenizer extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final LongWritable ONE = new LongWritable(1);
        private final Text word = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokens = new StringTokenizer(line.toString());
            while (tokens.hasMoreTokens()) {
                word.set(tokens.nextToken());
                context.write(word, ONE);
            }
        }
    }

    public static final class Sum extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text word, Iterable<LongWritable> counts, Context context)
                throws IOException, InterruptedException {
            long total = 0;
            for (LongWritable count : counts) {
                total += count.get();
            }
            context.write(word, new LongWritable(total));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: WordCountHadoop <fichier> <dossier-sortie>");
        }

        Configuration config = new Configuration();
        config.set("fs.defaultFS", "file:///");
        config.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(config, "TP1 - Comptage de mots");
        job.setJarByClass(WordCountHadoop.class);
        job.setMapperClass(Tokenizer.class);
        job.setCombinerClass(Sum.class);
        job.setReducerClass(Sum.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(false)) {
            throw new IOException("Le job Hadoop MapReduce a échoué.");
        }
    }
}
