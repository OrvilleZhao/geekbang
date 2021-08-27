package com.orville;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.FileNotFoundException;
import java.io.IOException;

public class CopyMapper extends Mapper<Text, FileStatus, Text, Text> {

    /**
     * Hadoop counters for the DistCp CopyMapper.
     * (These have been kept identical to the old DistCp,
     * for backward compatibility.)
     */
    public static enum Counter {
        COPY,         // Number of files received by the mapper for copy.
        SKIP,         // Number of files skipped.
        FAIL,         // Number of files that failed to be copied.
        BYTESCOPIED,  // Number of bytes actually copied by the copy-mapper, total.
        BYTESEXPECTED,// Number of bytes expected to be copied.
        BYTESFAILED,  // Number of bytes that failed to be copied.
        BYTESSKIPPED, // Number of bytes that were skipped from copy.
    }

    /**
     * Indicate the action for each file
     */
    static enum FileAction {
        SKIP,         // Skip copying the file since it's already in the target FS
        APPEND,       // Only need to append new data to the file in the target FS
        OVERWRITE,    // Overwrite the whole file
    }

    private static Log LOG = LogFactory.getLog(CopyMapper.class);

    private Configuration conf;

    private boolean syncFolders = false;
    private boolean ignoreFailures = false;
    private boolean skipCrc = false;
    private boolean overWrite = false;
    private boolean append = false;

    private FileSystem targetFS = null;
    private Path targetWorkPath = null;

    /**
     * Implementation of the Mapper::setup() method. This extracts the DistCp-
     * options specified in the Job's configuration, to set up the Job.
     * @param context Mapper's context.
     * @throws IOException On IO failure.
     * @throws InterruptedException If the job is interrupted.
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();

        ignoreFailures = conf.getBoolean("distcp.ignore.failures", false);

        targetWorkPath = new Path(conf.get("distcp.target.work.path"));
        Path targetFinalPath = new Path(conf.get("distcp.target.final.path"));
        targetFS = targetFinalPath.getFileSystem(conf);

        if (targetFS.exists(targetFinalPath) && targetFS.isFile(targetFinalPath)) {
            overWrite = true; // When target is an existing file, overwrite it.
        }
    }


    /**
     * Find entry from distributed cache
     *
     * @param cacheFiles - All localized cache files
     * @param fileName - fileName to search
     * @return Path of the filename if found, else null
     */
    private Path findCacheFile(Path[] cacheFiles, String fileName) {
        if (cacheFiles != null && cacheFiles.length > 0) {
            for (Path file : cacheFiles) {
                if (file.getName().equals(fileName)) {
                    return file;
                }
            }
        }
        return null;
    }

    /**
     * Implementation of the Mapper::map(). Does the copy.
     * @param relPath The target path.
     * @param sourceFileStatus The source path.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text relPath, FileStatus sourceFileStatus,
                    Context context) throws IOException, InterruptedException {
        Path sourcePath = sourceFileStatus.getPath();

        if (LOG.isDebugEnabled())
            LOG.debug("DistCpMapper::map(): Received " + sourcePath + ", " + relPath);

        Path target = new Path(targetWorkPath.makeQualified(targetFS.getUri(),
            targetFS.getWorkingDirectory()) + relPath.toString());

        final String description = "Copying " + sourcePath + " to " + target;
        context.setStatus(description);

        LOG.info(description);

        try {
            FileSystem sourceFS = sourcePath.getFileSystem(conf);

            FileStatus targetStatus = null;
            FileStatus sourceCurrStatus = sourceFS.getFileStatus(sourcePath);
            try {
                targetStatus = targetFS.getFileStatus(target);
            } catch (FileNotFoundException ignore) {
                if (LOG.isDebugEnabled())
                    LOG.debug("Path could not be found: " + target, ignore);
            }

            if (targetStatus != null && (targetStatus.isDirectory() != sourceCurrStatus.isDirectory())) {
                throw new IOException("Can't replace " + target + ". Target is " +
                    getFileType(targetStatus) + ", Source is " + getFileType(sourceCurrStatus));
            }

            if (targetStatus.isDirectory()) {
                createTargetDirsWithRetry(description, target, context);
                return;
            }


            FileAction action = checkUpdate(sourceFS, sourceCurrStatus, target);
            if (action == FileAction.SKIP) {
                LOG.info("Skipping copy of " + sourceCurrStatus.getPath()
                    + " to " + target);
                updateSkipCounters(context, sourceCurrStatus);
                context.write(null, new Text("SKIP: " + sourceCurrStatus.getPath()));
            } else {
                copyFileWithRetry(description, sourceCurrStatus, target, context, action);
            }
        } catch (IOException exception) {
            handleFailures(exception, sourceFileStatus, target, context);
        }
    }

    private String getFileType(FileStatus fileStatus) {
        return fileStatus == null ? "N/A" : (fileStatus.isDirectory() ? "dir" : "file");
    }

    private void copyFileWithRetry(String description,
                                   FileStatus sourceFileStatus, Path target, Context context,
                                   FileAction action)
        throws IOException {
        long bytesCopied;
        try {
            bytesCopied = (Long) new RetriableFileCopyCommand(skipCrc, description,
                action).execute(sourceFileStatus, target, context);
        } catch (Exception e) {
            context.setStatus("Copy Failure: " + sourceFileStatus.getPath());
            throw new IOException("File copy failed: " + sourceFileStatus.getPath() +
                " --> " + target, e);
        }
        incrementCounter(context, Counter.BYTESEXPECTED, sourceFileStatus.getLen());
        incrementCounter(context, Counter.BYTESCOPIED, bytesCopied);
        incrementCounter(context, Counter.COPY, 1);
    }

    private void createTargetDirsWithRetry(String description,
                                           Path target, Context context) throws IOException {
        try {
            new com.orville.RetriableDirectoryCreateCommand(description).execute(target, context);
        } catch (Exception e) {
            throw new IOException("mkdir failed for " + target, e);
        }
        incrementCounter(context, Counter.COPY, 1);
    }

    private static void updateSkipCounters(Context context,
                                           FileStatus sourceFile) {
        incrementCounter(context, Counter.SKIP, 1);
        incrementCounter(context, Counter.BYTESSKIPPED, sourceFile.getLen());

    }

    private void handleFailures(IOException exception,
                                FileStatus sourceFileStatus, Path target,
                                Context context) throws IOException, InterruptedException {
        LOG.error("Failure in copying " + sourceFileStatus.getPath() + " to " +
            target, exception);

        if (ignoreFailures && exception.getCause() instanceof
            RetriableFileCopyCommand.CopyReadException) {
            incrementCounter(context, Counter.FAIL, 1);
            incrementCounter(context, Counter.BYTESFAILED, sourceFileStatus.getLen());
            context.write(null, new Text("FAIL: " + sourceFileStatus.getPath() + " - " +
                StringUtils.stringifyException(exception)));
        }
        else
            throw exception;
    }

    private static void incrementCounter(Context context, Counter counter,
                                         long value) {
        context.getCounter(counter).increment(value);
    }

    private FileAction checkUpdate(FileSystem sourceFS, FileStatus source,
                                   Path target) throws IOException {
        final FileStatus targetFileStatus;
        try {
            targetFileStatus = targetFS.getFileStatus(target);
        } catch (FileNotFoundException e) {
            return FileAction.OVERWRITE;
        }
        if (targetFileStatus != null && !overWrite) {
            if (canSkip(sourceFS, source, targetFileStatus)) {
                return FileAction.SKIP;
            } else if (append) {
                long targetLen = targetFileStatus.getLen();
                if (targetLen < source.getLen()) {
                    return FileAction.APPEND;
                }
            }
        }
        return FileAction.OVERWRITE;
    }

    private boolean canSkip(FileSystem sourceFS, FileStatus source,
                            FileStatus target) throws IOException {
        if (!syncFolders) {
            return true;
        }
        boolean sameLength = target.getLen() == source.getLen();
        boolean sameBlockSize = source.getBlockSize() == target.getBlockSize();
        if (sameLength && sameBlockSize) {
            return skipCrc;
        } else {
            return false;
        }
    }
}
