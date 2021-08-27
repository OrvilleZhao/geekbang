package com.orville;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

public class RetriableDirectoryCreateCommand extends com.orville.RetriableCommand {

    /**
     * Constructor, taking a description of the action.
     * @param description Verbose description of the copy operation.
     */
    public RetriableDirectoryCreateCommand(String description) {
        super(description);
    }

    /**
     * Implementation of RetriableCommand::doExecute().
     * This implements the actual mkdirs() functionality.
     * @param arguments Argument-list to the command.
     * @return Boolean. True, if the directory could be created successfully.
     * @throws Exception IOException, on failure to create the directory.
     */
    @Override
    protected Object doExecute(Object... arguments) throws Exception {
        assert arguments.length == 2 : "Unexpected argument list.";
        Path target = (Path)arguments[0];
        Mapper.Context context = (Mapper.Context)arguments[1];

        FileSystem targetFS = target.getFileSystem(context.getConfiguration());
        return targetFS.mkdirs(target);
    }
}
