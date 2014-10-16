package eu.scape_project.audio_qa.souind_index;

/**
 * A class containing constants for use elsewhere in the code.
 * eu.scape_project
 * User: baj@statsbiblioteket.dk
 * Date: 8/6/13
 */
public class SoundIndexSettings {

    /**
     * String constants.
     */
    public static final String MPG321 = "mpg321";
    public static final String SLASH = "/";
    public static final String UNDERSCORE = "_";
    public static final String DOTLOG = ".log";
    public static final String DOTWAV = ".wav";

    /**
     * Default workflow output directories on HDFS and NFS.
     * TODO put all results on NFS
     */
    public static String MAPPER_OUTPUT_DIR = "baj/out/";//baj SB Hadoop cluster setting
            //"hdfs:///user/bolette/output/test-output/MigrateMp3ToWav/";//bolette-ubuntu setting
    public static String TOOL_OUTPUT_DIR = "/scape/shared/out/wav/";//baj SB scape@iapetus setting
            //"/home/bolette/TestOutput/";//bolette-ubuntu setting

    /**
     * Default job id used to create a job specific output directory.
     */
    public static String DEFAULT_JOBID = "test-default-jobid";
}
