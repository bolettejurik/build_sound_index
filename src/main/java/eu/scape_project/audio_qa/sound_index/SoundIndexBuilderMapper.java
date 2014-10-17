package eu.scape_project.audio_qa.sound_index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.net.URI;

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

    private Log log = new Log4JLogger(SoundIndexBuilderMapper.class.getName());

    @Override
    protected void map(LongWritable lineNo, Text inputMp3path, Context context) throws IOException, InterruptedException {

        if (inputMp3path.toString().equals("")) return;

        //get input mp3 name
        String inputMp3 = new File(inputMp3path.toString()).getName().replace(".mp3","");
        String databaseName = new File(inputMp3path.toString()).getName().replace(".mp3",".db");

        //create a hadoop-job-specific output dir on hdfs
        String ffmpegOutputDir;
        if (context.getJobID() == null) {
            ffmpegOutputDir = context.getConfiguration().get(SoundIndexBuilder.FFMPEG_OUTPUTDIR, SoundIndexSettings.FFMPEG_DEFAULT) +
                    context.getConfiguration().get("job.jobID", SoundIndexSettings.DEFAULT_JOBID);
        } else {
            ffmpegOutputDir = context.getConfiguration().get(SoundIndexBuilder.FFMPEG_OUTPUTDIR, SoundIndexSettings.FFMPEG_DEFAULT) +
                    context.getJobID().toString();
        }

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

        new File(ffmpegOutputDir).mkdirs();
        try {
            String outputwavPath = migrate(inputMp3path, context, inputMp3, ffmpegOutputDir, fs);

            buildIndex(outputwavPath, databaseName, indexPath, fs);
            rmWav(outputwavPath, fs);
            context.write(new LongWritable(0), new Text(databaseName));
        } catch (IOException e){
            log.error("Caught IOException",e);
            context.write(new LongWritable(1), inputMp3path);
        }

    }

    private void rmWav(String outputwavPath, FileSystem fs) throws IOException {

        String[] rmCommand = new String[]{
                "rm",
                "-f",
                outputwavPath,
        };

        final Text output = new Text();

        int exitCode = CLIToolRunner.runCLItool(rmCommand, null,fs,null,output);
        if (exitCode != 0) {
            throw new IOException(output.toString());
        }
    }

    private void buildIndex(String outputwavPath, String databaseName, File indexPath, FileSystem fs) throws IOException {
        //build index with ismir_build_index
        String ismirBuildIndexLog = indexPath.getAbsolutePath() + "/" + databaseName + "_ismir.log";

        String[] ismirBuildIndexCommand = new String[]{
                "/home/scape/bin/ismir_build_index",
                "-d",
                databaseName,
                "-i",
                outputwavPath,
        };
        final Text output = new Text();
        int exitCode = CLIToolRunner.runCLItool(ismirBuildIndexCommand, ismirBuildIndexLog, fs,indexPath, output);
        if (exitCode != 0) {
            throw new IOException(output.toString());
        }


    }

    private String migrate(Text inputMp3path, Context context, String inputMp3, String outputDirPath, FileSystem fs) throws IOException {
        //migrate with ffmpeg
        String ffmpeglog = outputDirPath + "/" + inputMp3 + "_ffmpeg.log";
        String outputwavPath = context.getConfiguration().get(SoundIndexBuilder.FFMPEG_OUTPUTDIR, SoundIndexSettings.FFMPEG_DEFAULT) +
                SoundIndexSettings.SLASH + inputMp3 + SoundIndexSettings.UNDERSCORE + "ffmpeg" + SoundIndexSettings.DOTWAV;

        String[] ffmpegcommand = new String[]{
                "ffmpeg",
                "-y",
                "-i",
                inputMp3path.toString(),
                "-ar", "5512",
                outputwavPath,
        };
        final Text output = new Text();

        int exitCode = CLIToolRunner.runCLItool(ffmpegcommand, ffmpeglog, fs, null,output);
        if (exitCode != 0) {
            throw new IOException(output.toString());
        }
        return outputwavPath;
    }

}