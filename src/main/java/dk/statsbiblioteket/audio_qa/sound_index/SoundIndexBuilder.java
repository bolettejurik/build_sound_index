package dk.statsbiblioteket.audio_qa.sound_index;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Create the map-reduce job for migrating the mp3 files on the given input list using ffmpeg.
 * dk.statsbiblioteket.audio_qa.souind_index
 * User: baj@statsbiblioteket.dk
 * Date: 2014-01-14
 */
public class SoundIndexBuilder extends Configured implements Tool {

    protected static final String ISMIR_OUTPUTDIR = "ismir.outputdir";
    protected static final String FFMPEG_OUTPUTDIR = "ffmpeg.outputdir";

    static public class SoundIndexBuilderReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void reduce(LongWritable exitCode, Iterable<Text> outputs, Context context)
                throws IOException, InterruptedException {
            Text list = new Text("");
            for (Text output : outputs) {
                list = new Text(list.toString() + output.toString() + "\n");
            }
            context.write(exitCode, list);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        configuration.set(NLineInputFormat.LINES_PER_MAP, "10");
        configuration.set("mapred.line.input.format.linespermap", "10");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(SoundIndexBuilder.class);


        int n = args.length;
        if (n > 0)
            NLineInputFormat.addInputPath(job, new Path(args[0]));
        if (n > 1)
            SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (n > 2)
            configuration.set(ISMIR_OUTPUTDIR, args[2]);
        if (n > 3)
            configuration.set(FFMPEG_OUTPUTDIR, args[3]);

        job.setMapperClass(SoundIndexBuilderMapper.class);
        job.setReducerClass(SoundIndexBuilderReducer.class);

        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        if (job.getJobID()==null) {configuration.set("job.jobID", "SoundIndexBuilder" + Math.random());}

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SoundIndexBuilder(), args));
    }
}
