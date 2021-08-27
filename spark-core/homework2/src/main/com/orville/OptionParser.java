/**
 * 仿照dsitcp库OptionsParser编写，主要做参数解析
 * 解析的参数主要为 Discp -
 */
package com.orville;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OptionParser {

    public static DistCpOptions parse(String args[]) {
        Options cliOptions = new Options();
        cliOptions.addOption("i", false, "Ignore failures during copy");
        cliOptions.addOption("m", true, "Max number of concurrent maps to use for copy");
        CommandLineParser parser = new GnuParser();
        CommandLine command;
        try {
            command = parser.parse(cliOptions, args, true);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Unable to parse arguments. " +
                Arrays.toString(args), e);
        }

        Path targetPath;
        List<Path> sourcePaths = new ArrayList<Path>();

        String leftOverArgs[] = command.getArgs();
        if (leftOverArgs == null || leftOverArgs.length < 1) {
            throw new IllegalArgumentException("Target path not specified");
        }

        //最后一个参数是目标path
        targetPath = new Path(leftOverArgs[leftOverArgs.length -1].trim());
        DistCpOptions option = new DistCpOptions(sourcePaths, targetPath); ;
        //源path列表
        for (int index = 0; index < leftOverArgs.length - 1; index++) {
            sourcePaths.add(new Path(leftOverArgs[index].trim()));
        }
        //看ignore failure是否开启
        if (command.hasOption("i")) {
            option.setIgnoreFailures(true);
        }
        //配置最大的map数量
        if (command.hasOption("m")) {
            String optionValue = command.getOptionValue("m");
            Integer maps = Integer.parseInt(optionValue);
            option.setMaxMaps(maps);
        }




        return null;
    }
}
