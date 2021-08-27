package com.orville;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Random;

public class DistCp extends Configured implements Tool {
    static final Log LOG = LogFactory.getLog(DistCp.class);

    private static final String PREFIX = "_distcp";

    static final Random rand = new Random();

    private Path metaFolder;

    private FileSystem jobFS;

    private boolean submitted;

    private DistCpOptions inputOptions;

    public int run(String[] argv) throws Exception {
        try {
            inputOptions = (OptionParser.parse(argv));
            setTargetPathExists();
            LOG.info("Input Options: " + inputOptions);
        } catch (Throwable e) {
            LOG.error("Invalid arguments: ", e);
            System.err.println("Invalid arguments: " + e.getMessage());
            OptionParser.usage();
            return -1;
        }
        Job job = createAndSubmitJob();
        return 0;
    }

    /**
     * Create and submit the mapreduce job.
     * @return The mapreduce job object that has been submitted
     */
    public Job createAndSubmitJob() throws Exception {
        assert inputOptions != null;
        assert getConf() != null;
        Job job = null;
        try {
            synchronized(this) {
                metaFolder = createMetaFolderPath();
                jobFS = metaFolder.getFileSystem(getConf());
                job = createJob();
            }

            job.submit();
            submitted = true;
        } finally {
            if (!submitted) {
                cleanup();
            }
        }

        String jobID = job.getJobID().toString();
        job.getConfiguration().set("distcp.job.id", jobID);
        LOG.info("DistCp job-id: " + jobID);

        return job;
    }

    private synchronized void cleanup() {
        try {
            if (metaFolder == null) return;

            jobFS.delete(metaFolder, true);
            metaFolder = null;
        } catch (IOException e) {
            LOG.error("Unable to cleanup meta folder: " + metaFolder, e);
        }
    }

    /**
     * Get default name of the copy listing file. Use the meta folder
     * to create the copy listing file
     *
     * @return - Path where the copy listing file has to be saved
     * @throws IOException - Exception if any
     */
    protected Path getFileListingPath() throws IOException {
        String fileListPathStr = metaFolder + "/fileList.seq";
        Path path = new Path(fileListPathStr);
        return new Path(path.toUri().normalize().toString());
    }

    /**
     * Create a default working folder for the job, under the
     * job staging directory
     *
     * @return Returns the working folder information
     * @throws Exception - EXception if any
     */
    private Path createMetaFolderPath() throws Exception {
        Configuration configuration = getConf();
        Path stagingDir = JobSubmissionFiles.getStagingDir(
            new Cluster(configuration), configuration);
        Path metaFolderPath = new Path(stagingDir, PREFIX + String.valueOf(rand.nextInt()));
        if (LOG.isDebugEnabled())
            LOG.debug("Meta folder location: " + metaFolderPath);
        configuration.set("distcp.meta.folder", metaFolderPath.toString());
        return metaFolderPath;
    }

    /**
     * Create Job object for submitting it, with all the configuration
     *
     * @return Reference to job object.
     * @throws IOException - Exception if any
     */
    private Job createJob() throws IOException {
        String jobName = "distcp";
        String userChosenName = getConf().get(JobContext.JOB_NAME);
        if (userChosenName != null)
            jobName += ": " + userChosenName;
        Job job = Job.getInstance(getConf());
        job.setJobName(jobName);
        job.setJarByClass(CopyMapper.class);
        configureOutputFormat(job);

        job.setMapperClass(CopyMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.getConfiguration().set(JobContext.NUM_MAPS,
            String.valueOf(inputOptions.getMaxMaps()));
        return job;
    }

    /**
     * Setup output format appropriately
     *
     * @param job - Job handle
     * @throws IOException - Exception if any
     */
    private void configureOutputFormat(Job job) throws IOException {
        final Configuration configuration = job.getConfiguration();
        Path targetPath = inputOptions.getTargetPath();
        FileSystem targetFS = targetPath.getFileSystem(configuration);
        targetPath = targetPath.makeQualified(targetFS.getUri(),
            targetFS.getWorkingDirectory());
        job.getConfiguration().set("distcp.target.work.path", targetPath.toString());
        job.getConfiguration().set("distcp.target.final.path", targetPath.toString());
        Path logPath = inputOptions.getLogPath();
        if (logPath == null) {
            logPath = new Path(metaFolder, "_logs");
        } else {
            LOG.info("DistCp job log path: " + logPath);
        }
        TextOutputFormat.setOutputPath(job, logPath);
    }


    /**
     * Set targetPathExists in both inputOptions and job config,
     * for the benefit of CopyCommitter
     */
    private void setTargetPathExists() throws IOException {
        Path target = inputOptions.getTargetPath();
        FileSystem targetFS = target.getFileSystem(getConf());
        boolean targetExists = targetFS.exists(target);
        inputOptions.setTargetPathExists(targetExists);
        getConf().setBoolean("distcp.target.path.exists", targetExists);
    }
}
