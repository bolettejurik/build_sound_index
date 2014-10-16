package eu.scape_project.audio_qa.souind_index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;

/**
 * The map function of eu.scape_project.audio_qa.souind_index.SoundIndexBuilderMapper migrates the mp3 files referenced in input to wav using ffmpeg.
 * The map function returns the path to the resulting wav file.
 * <p/>
 * The input is a line number as key (not used) and a Text line, which we assume is the path to an mp3 file.
 * The output is an exit code (not used), and the path to an output file.
 * <p/>
 * eu.scape_project.audio_qa.souind_index
 * User: baj@statsbiblioteket.dk
 * Date: 2014-01-14
 */
public class SoundIndexBuilderMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private Log log = new Log4JLogger("eu.scape_project.audio_qa.souind_index.SoundIndexBuilderMapper Log");

    @Override
    protected void map(LongWritable lineNo, Text inputMp3path, Context context) throws IOException, InterruptedException {

        if (inputMp3path.toString().equals("")) return;

        //get input mp3 name
        String inputMp3 = new File(inputMp3path.toString()).getName().replace(".mp3","");
        String databaseName = new File(inputMp3path.toString()).getName().replace(".mp3",".db");

        //create a hadoop-job-specific output dir on hdfs
        String outputDirPath;
        if (context.getJobID() == null) {
            outputDirPath = context.getConfiguration().get("map.outputdir", SoundIndexSettings.MAPPER_OUTPUT_DIR) +
                    context.getConfiguration().get("job.jobID", SoundIndexSettings.DEFAULT_JOBID);
        } else {
            outputDirPath = context.getConfiguration().get("map.outputdir", SoundIndexSettings.MAPPER_OUTPUT_DIR) +
                    context.getJobID().toString();
        }
        File indexPath = new File("/scape/shared/out/lydindex/");

        FileSystem fs = FileSystem.get(context.getConfiguration());
        boolean succesfull = fs.mkdirs(new Path(outputDirPath));
        log.debug(outputDirPath + "\nfs.mkdirs " + succesfull);

        String outputwavPath = migrate(inputMp3path, context, inputMp3, outputDirPath, fs);

        buildIndex(outputwavPath, databaseName, outputDirPath, indexPath, fs);
        rmWav(outputwavPath, fs);

        context.write(new LongWritable(0), new Text(databaseName.toString()));
    }

    private void rmWav(String outputwavPath, FileSystem fs) throws IOException {

        String[] rmCommand = new String[]{
                "rm",
                "-f",
                outputwavPath,
        };
        int exitCode = CLIToolRunner.runCLItool(rmCommand, null,fs,null,new Text());

    }

    private void buildIndex(String outputwavPath, String databaseName, String outputDirPath, File indexPath, FileSystem fs) throws IOException {
        //build index with ismir_build_index
        String ismirBuildIndexLog = outputDirPath + "/" + databaseName + "_ismir.log";

        String[] ismirBuildIndexCommand = new String[]{
                "ismir_build_index",
                "-d",
                databaseName,
                "-i",
                outputwavPath,
        };
        int exitCode = CLIToolRunner.runCLItool(ismirBuildIndexCommand, ismirBuildIndexLog, fs,indexPath, new Text());
        //TODO log exitcode

    }

    private String migrate(Text inputMp3path, Context context, String inputMp3, String outputDirPath, FileSystem fs) throws IOException {
        //migrate with ffmpeg
        String ffmpeglog = outputDirPath + "/" + inputMp3 + "_ffmpeg.log";
        String outputwavPath = context.getConfiguration().get("tool.outputdir", SoundIndexSettings.TOOL_OUTPUT_DIR) +
                SoundIndexSettings.SLASH + inputMp3 + SoundIndexSettings.UNDERSCORE + "ffmpeg" + SoundIndexSettings.DOTWAV;

        String[] ffmpegcommand = new String[]{
                "ffmpeg",
                "-y",
                "-ar",
                "5512",
                "-i",
                inputMp3path.toString(),
                outputwavPath,
        };
        int exitCode = CLIToolRunner.runCLItool(ffmpegcommand, ffmpeglog, fs, null,new Text());
        //TODO log exitcode
        //Note ffmpeg will not overwrite earlier results when we do not explicitly allow it!
        return outputwavPath;
    }

}