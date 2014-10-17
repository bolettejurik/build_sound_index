package dk.statsbiblioteket.audio_qa.sound_index;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * SoundIndexMergerReducer merges the sound indexes from the reducer input into fewer indexes.
 * In this first solution, we take 100 indexes and merge into 1.
 *
 * dk.statsbiblioteket.audio_qa.sound_index
 * User: baj@statsbiblioteket.dk
 * Date: 10/17/14
 * Time: 12:41 PM
 */
public class SoundIndexMergerReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void reduce(LongWritable exitCode, Iterable<Text> indexPathIterable, Context context)
            throws IOException, InterruptedException {

        String ismir_workingDir;
        if (context.getJobID() == null) {
            ismir_workingDir = context.getConfiguration()
                    .get(SoundIndexBuilder.ISMIR_OUTPUTDIR, SoundIndexSettings.ISMIR_DEFAULT)  + context.getConfiguration()
                    .get("job.jobID",
                            SoundIndexSettings.DEFAULT_JOBID);
        } else {
            ismir_workingDir = context.getConfiguration()
                    .get(SoundIndexBuilder.ISMIR_OUTPUTDIR, SoundIndexSettings.ISMIR_DEFAULT)  + context.getJobID()
                    .toString();
        }

        FileSystem fs = FileSystem.get(URI.create("file:///"),context.getConfiguration());

        File indexPath = new File(ismir_workingDir);
        indexPath.mkdirs();

        Iterator indexPathIterator = indexPathIterable.iterator();
        String newIndiceslist = "";

        while (indexPathIterator.hasNext()) {
            //merge indexes into new index with ismir_merge
            String ismirMergeLog;
            String ismirMergeIndex;
            List<String> ismirMergeCommand = new ArrayList<String>();

            if (indexPathIterator.hasNext()) {
                String next = indexPathIterator.next().toString();
                File nextFile = new File(next);
                ismirMergeLog = ismir_workingDir + "merge_from_" + nextFile.getName().replace(".db",".log");
                ismirMergeIndex = ismir_workingDir + "merge_from_" + nextFile.getName();

                ismirMergeCommand.add("/home/scape/bin/ismir_merge");
                ismirMergeCommand.add("-i");
                ismirMergeCommand.add(next);
            } else {
                return;
            }


            int counter = 0;
            while (indexPathIterator.hasNext() && counter++ <= 100) {
                ismirMergeCommand.add(indexPathIterator.next().toString());
            }
            ismirMergeCommand.add("-o");
            ismirMergeCommand.add(ismirMergeIndex);

            final Text toolRunnerOutput = new Text();
            int toolRunnerExitCode = CLIToolRunner.runCLItool(ismirMergeCommand.toArray(new String[0]), ismirMergeLog, fs,indexPath, toolRunnerOutput);
            if (toolRunnerExitCode != 0) {
                throw new RuntimeException(toolRunnerOutput.toString());
            }
            newIndiceslist += (ismirMergeIndex + "\n");
        }

        context.write(new LongWritable(0), new Text(newIndiceslist));
    }
}
