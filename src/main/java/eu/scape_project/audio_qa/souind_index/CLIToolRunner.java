package eu.scape_project.audio_qa.souind_index;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * eu.scape_project
 * User: baj@statsbiblioteket.dk
 * Date: 8/7/13
 * Time: 11:57 AM
 */
public class CLIToolRunner {
    public static int runCLItool(String[] commandline, String logFile, FileSystem fs, File workingDir, Text output) throws IOException {
        // todo localize parameters??? or use tomar???

        ProcessBuilder pb = new ProcessBuilder(commandline);
        if (workingDir != null){
            pb.directory(workingDir);
        }
        //start the executable
        Process proc = pb.start();
        BufferedReader stdout = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        BufferedReader stderr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        try {
            //wait for process to end before continuing
            proc.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        int exitCode = proc.exitValue();
        String stdoutString = "";
        while (stdout.ready()) {
            stdoutString += stdout.readLine() + "\n";
        }
        String stderrString = "";
        while (stderr.ready()) {
            stderrString += stderr.readLine() + "\n";
        }

        if (logFile != null) {
            //TODO write log of stdout and stderr to the log file
            FSDataOutputStream out = fs.create(new Path(logFile));
            out.writeBytes(stdoutString + stderrString);
            out.flush();
            out.close();
        }

        if (output == null) output = new Text();
        output.set(stdoutString + stderrString);
        return exitCode;
    }
}
