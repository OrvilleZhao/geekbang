package com.orville;

import org.apache.hadoop.fs.Path;

import java.util.List;

public class DistCpOptions {

    private List<Path> sourcePaths;

    private Path targetPath;

    private boolean ignoreFailures;

    private int maxMaps;

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
}
