package com.orville;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.List;

public class DistCpOptions {

    private List<Path> sourcePaths;

    private Path targetPath;

    private boolean ignoreFailures;

    private int maxMaps;

    private boolean targetPathExists;

    private Path logPath;

    public DistCpOptions(List<Path> sourcePaths, Path targetPath) {
        assert sourcePaths != null && !sourcePaths.isEmpty() : "Invalid source paths";
        assert targetPath != null : "Invalid Target path";

        this.sourcePaths = sourcePaths;
        this.targetPath = targetPath;
    }

    public void setIgnoreFailures(boolean ignoreFailures) {
        this.ignoreFailures = ignoreFailures;
    }

    public void setMaxMaps(int maxMaps) {
        this.maxMaps = Math.max(maxMaps, 1);
    }

    public Path getTargetPath() {
        return this.targetPath;
    }

    public void setTargetPathExists(boolean targetPathExists) {
        this.targetPathExists = targetPathExists;
    }

    public int getMaxMaps() {
        return this.maxMaps;
    }

    /** Get output directory for writing distcp logs. Otherwise logs
     * are temporarily written to JobStagingDir/_logs and deleted
     * upon job completion
     *
     * @return Log output path on the cluster where distcp job is run
     */
    public Path getLogPath() {
        return logPath;
    }

    /**
     * Set the log path where distcp output logs are stored
     * Uses JobStagingDir/_logs by default
     *
     * @param logPath - Path where logs will be saved
     */
    public void setLogPath(Path logPath) {
        this.logPath = logPath;
    }

}
