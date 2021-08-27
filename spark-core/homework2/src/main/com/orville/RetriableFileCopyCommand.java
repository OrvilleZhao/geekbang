package com.orville;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RetriableFileCopyCommand extends com.orville.RetriableCommand {

    private static Logger LOG = LoggerFactory.getLogger(RetriableFileCopyCommand.class);
    private static int BUFFER_SIZE = 8 * 1024;
    private boolean skipCrc = false;
    private boolean directWrite = false;
    private com.orville.CopyMapper.FileAction action;

    /**
     * Constructor, taking a description of the action.
     * @param description Verbose description of the copy operation.
     */
    public RetriableFileCopyCommand(String description, com.orville.CopyMapper.FileAction action) {
        super(description);
        this.action = action;
    }

    /**
     * Create a RetriableFileCopyCommand.
     *
     * @param skipCrc Whether to skip the crc check.
     * @param description A verbose description of the copy operation.
     * @param action We should overwrite the target file or append new data to it.
     */
    public RetriableFileCopyCommand(boolean skipCrc, String description,
                                    com.orville.CopyMapper.FileAction action) {
        this(description, action);
        this.skipCrc = skipCrc;
    }

    /**
     * Create a RetriableFileCopyCommand.
     *
     * @param skipCrc Whether to skip the crc check.
     * @param description A verbose description of the copy operation.
     * @param action We should overwrite the target file or append new data to it.
     * @param directWrite Whether to write directly to the target path, avoiding a
     *                    temporary file rename.
     */
    public RetriableFileCopyCommand(boolean skipCrc, String description,
                                    com.orville.CopyMapper.FileAction action, boolean directWrite) {
        this(skipCrc, description, action);
        this.directWrite = directWrite;
    }

    /**
     * Implementation of RetriableCommand::doExecute().
     * This is the actual copy-implementation.
     * @param arguments Argument-list to the command.
     * @return Number of bytes copied.
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Override
    protected Object doExecute(Object... arguments) throws Exception {
        assert arguments.length == 5 : "Unexpected argument list.";
        FileStatus source = (FileStatus)arguments[0];
        assert !source.isDirectory() : "Unexpected file-status. Expected file.";
        Path target = (Path)arguments[1];
        Mapper.Context context = (Mapper.Context)arguments[2];
        FileStatus sourceStatus = (FileStatus)arguments[4];
        return doCopy(source, target, context, sourceStatus);
    }

    private long doCopy(FileStatus source, Path target,
                        Mapper.Context context,
                        FileStatus sourceStatus)
        throws IOException {
        LOG.info("Copying {} to {}", source.getPath(), target);

        final boolean toAppend = action == com.orville.CopyMapper.FileAction.APPEND;
        final boolean useTempTarget = !toAppend && !directWrite;
        Path targetPath = useTempTarget ? getTempFile(target, context) : target;

        LOG.info("Writing to {} target file path {}", useTempTarget ? "temporary"
            : "direct", targetPath);

        final Configuration configuration = context.getConfiguration();
        FileSystem targetFS = target.getFileSystem(configuration);

        try {
            final Path sourcePath = source.getPath();
            final FileSystem sourceFS = sourcePath.getFileSystem(configuration);

            long offset = targetFS.getFileStatus(target).getLen();
            long bytesRead = copyToFile(targetPath, targetFS, source,
                offset, context);

            // it's not append or direct write (preferred for s3a) case, thus we first
            // write to a temporary file, then rename it to the target path.
            if (useTempTarget) {
                LOG.info("Renaming temporary target file path {} to {}", targetPath,
                    target);
                promoteTmpToTarget(targetPath, target, targetFS);
            }
            LOG.info("Completed writing {} ({} bytes)", target, bytesRead);
            return bytesRead;
        } finally {
            // note that for append case, it is possible that we append partial data
            // and then fail. In that case, for the next retry, we either reuse the
            // partial appended data if it is good or we overwrite the whole file
            if (useTempTarget) {
                targetFS.delete(targetPath, false);
            }
        }
    }

    private long copyToFile(Path targetPath, FileSystem targetFS,
                            FileStatus sourceFileStatus, long sourceOffset, Mapper.Context context)
        throws IOException {
        FsPermission permission = FsPermission.getFileDefault().applyUMask(
            FsPermission.getUMask(targetFS.getConf()));
        final OutputStream outStream;
        if (action == com.orville.CopyMapper.FileAction.OVERWRITE) {
            final short repl = getReplicationFactor(targetFS, targetPath);
            final long blockSize = getBlockSize(targetFS, targetPath);
            //默认覆盖
            FSDataOutputStream out = targetFS.create(targetPath,true, BUFFER_SIZE, repl, blockSize, context);
            outStream = new BufferedOutputStream(out);
        } else {
            outStream = new BufferedOutputStream(targetFS.append(targetPath,
                BUFFER_SIZE));
        }
        return copyBytes(sourceFileStatus, sourceOffset, outStream, BUFFER_SIZE,
            context);
    }

    //If target file exists and unable to delete target - fail
    //If target doesn't exist and unable to create parent folder - fail
    //If target is successfully deleted and parent exists, if rename fails - fail
    private void promoteTmpToTarget(Path tmpTarget, Path target, FileSystem fs)
        throws IOException {
        if ((fs.exists(target) && !fs.delete(target, false))
            || (!fs.exists(target.getParent()) && !fs.mkdirs(target.getParent()))
            || !fs.rename(tmpTarget, target)) {
            throw new IOException("Failed to promote tmp-file:" + tmpTarget
                + " to: " + target);
        }
    }

    private Path getTempFile(Path target, Mapper.Context context) {
        Path targetWorkPath = new Path(context.getConfiguration().get("distcp.target.work.path"));

        Path root = target.equals(targetWorkPath) ? targetWorkPath.getParent()
            : targetWorkPath;
        Path tempFile = new Path(root, ".distcp.tmp." +
            context.getTaskAttemptID().toString() +
            "." + String.valueOf(System.currentTimeMillis()));
        LOG.info("Creating temp file: {}", tempFile);
        return tempFile;
    }

    @VisibleForTesting
    long copyBytes(FileStatus source2, long sourceOffset,
                   OutputStream outStream, int bufferSize, Mapper.Context context)
        throws IOException {
        Path source = source2.getPath();
        byte buf[] = new byte[bufferSize];
        InputStream inStream = null;
        long totalBytesRead = 0;

        boolean finished = false;
        try {
            inStream = getInputStream(source, context.getConfiguration());
            long fileLength = source2.getLen();
            int numBytesToRead  = (int) getNumBytesToRead(fileLength, sourceOffset,
                bufferSize);
            int bytesRead = readBytes(inStream, buf, numBytesToRead);
            while (bytesRead > 0) {
                totalBytesRead += bytesRead;
                sourceOffset += bytesRead;
                outStream.write(buf, 0, bytesRead);
                if (finished) {
                    break;
                }
                numBytesToRead  = (int) getNumBytesToRead(fileLength, sourceOffset,
                    bufferSize);
                bytesRead = readBytes(inStream, buf, numBytesToRead);
            }
            outStream.close();
            outStream = null;
        } finally {
            IOUtils.cleanupWithLogger(LOG, outStream, inStream);
        }
        return totalBytesRead;
    }

    @VisibleForTesting
    long getNumBytesToRead(long fileLength, long position, long bufLength) {
        if (position + bufLength < fileLength) {
            return  bufLength;
        } else {
            return fileLength - position;
        }
    }

    private static int readBytes(InputStream inStream, byte[] buf,
                                 int numBytes)
        throws IOException {
        try {
            return inStream.read(buf, 0, numBytes);
        } catch (IOException e) {
            throw new CopyReadException(e);
        }
    }

    private static InputStream getInputStream(Path path,
                                                       Configuration conf) throws IOException {
        try {
            FileSystem fs = path.getFileSystem(conf);
            FSDataInputStream in = fs.open(path);
            return in;
        }
        catch (IOException e) {
            throw new CopyReadException(e);
        }
    }

    private static short getReplicationFactor(FileSystem targetFS, Path tmpTargetPath) {
        return targetFS.getDefaultReplication(tmpTargetPath);
    }

    /**
     * @return the block size of the source file if we need to preserve either
     *         the block size or the checksum type. Otherwise the default block
     *         size of the target FS.
     */
    private static long getBlockSize(FileSystem targetFS, Path tmpTargetPath) {
        return targetFS.getDefaultBlockSize(tmpTargetPath);
    }

    /**
     * Special subclass of IOException. This is used to distinguish read-operation
     * failures from other kinds of IOExceptions.
     * The failure to read from source is dealt with specially, in the CopyMapper.
     * Such failures may be skipped if the DistCpOptions indicate so.
     * Write failures are intolerable, and amount to CopyMapper failure.
     */
    @SuppressWarnings("serial")
    public static class CopyReadException extends IOException {
        public CopyReadException(Throwable rootCause) {
            super(rootCause);
        }
    }
}
