package dk.statsbiblioteket.audio_qa.sound_index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Random;

/**
 * dk.statsbiblioteket.audio_qa.sound_index
 * User: baj@statsbiblioteket.dk
 * Date: 10/17/14
 * Time: 11:21 AM
 */
public class SoundIndexMerger extends Configured implements Tool {
    static public class SoundIndexMergerMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private Log log = new Log4JLogger("dk.statsbiblioteket.audio_qa.souind_index.SoundIndexMergerMapper Log");

        @Override
        protected void map(LongWritable lineNo, Text inputIndexPath, Context context) throws IOException, InterruptedException {

            if (inputIndexPath.toString().equals("")) return;

            if (inputIndexPath.toString().startsWith("map_")) return; //will be handled together with regular index file

            //get input index name
            String inputIndex = new File(inputIndexPath.toString()).getName().replace(".db", "");

            context.write(new LongWritable(new Random(new Date().getTime()).nextInt(100)), inputIndexPath);
        }

    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        configuration.set(NLineInputFormat.LINES_PER_MAP, "10");
        configuration.set("mapred.line.input.format.linespermap", "10");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(SoundIndexMerger.class);


        int n = args.length;
        if (n > 0)
            NLineInputFormat.addInputPath(job, new Path(args[0]));
        if (n > 1)
            TextOutputFormat.setOutputPath(job, new Path(args[1]));
        if (n > 2)
            configuration.set(SoundIndexBuilder.ISMIR_OUTPUTDIR, args[2]);
        if (n > 3)
            configuration.set(SoundIndexBuilder.FFMPEG_OUTPUTDIR, args[3]);

        job.setMapperClass(SoundIndexMergerMapper.class);
        job.setReducerClass(SoundIndexMergerReducer.class);

        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        if (job.getJobID()==null) {configuration.set("job.jobID", "SoundIndexMerger" + Math.random());}

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SoundIndexMerger(), args));
    }}
