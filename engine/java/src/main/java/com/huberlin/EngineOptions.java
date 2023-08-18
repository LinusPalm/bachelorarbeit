package com.huberlin;

import org.apache.commons.cli.*;

import java.io.File;

public class EngineOptions {
    private final String ownNodeId;
    private final String networkConfigPath;
    private final String planPath;
    private final boolean withWebUi;

    private final String flinkConfigDir;
    private final boolean noSelectivity;

    public EngineOptions(String ownNodeId,
                         String networkConfigPath,
                         String planPath,
                         boolean withWebUi,
                         String flinkConfigDir,
                         boolean noSelectivity) {
        this.ownNodeId = ownNodeId;
        this.networkConfigPath = networkConfigPath;
        this.planPath = planPath;
        this.withWebUi = withWebUi;
        this.flinkConfigDir = flinkConfigDir;
        this.noSelectivity = noSelectivity;
    }

    public String getOwnNodeId() { return this.ownNodeId; }

    public String getNetworkConfigPath() { return this.networkConfigPath; }

    public String getPlanPath() { return this.planPath; }

    public boolean getWithWebUi() { return this.withWebUi; }

    public String getFlinkConfigDir() { return this.flinkConfigDir; }

    public boolean getNoSelectivity() { return this.noSelectivity; }

    public static EngineOptions parse(String[] args) {
        Options argOptions = new Options();
        argOptions.addOption(Option.builder("n")
                .longOpt("node").argName("nodeId").hasArg().required()
                .desc("Id of this node").build());

        argOptions.addOption(Option.builder("c")
                .longOpt("config").argName("config path").hasArg().required()
                .desc("Path to network config json file").build());

        argOptions.addOption(Option.builder("p")
                .longOpt("plan").argName("plan path").hasArg().required()
                .desc("Path to query plan json file").build());

        argOptions.addOption(Option.builder("webui").desc("Enable flink web interface").build());
        argOptions.addOption(Option.builder("fc").longOpt("flink_config").hasArg().desc("Path to flink config directory").build());
        argOptions.addOption(Option.builder("no_selectivity").desc("Set all selectivities to 1.").build());

        CommandLineParser argParser = new DefaultParser();
        CommandLine parsed = null;
        try {
            parsed = argParser.parse(argOptions, args);
        } catch (ParseException e) {
            System.err.println("Invalid arguments." + e.getMessage());
            return null;
        }

        String nodeId = parsed.getOptionValue('n');
        String configPath = parsed.getOptionValue('c');
        String planPath = parsed.getOptionValue('p');
        boolean withWebUi = parsed.hasOption("webui");
        String flinkConfigDir = parsed.getOptionValue("flink_config", "conf");
        boolean noSelectivity = parsed.hasOption("no_selectivity");

        if (!new File(configPath).exists()) {
            System.err.println("Network config file does not exist.");
            return null;
        }

        if (!new File(planPath).exists()) {
            System.err.println("Query plan file does not exist.");
            return null;
        }

        return new EngineOptions(nodeId, configPath, planPath, withWebUi, flinkConfigDir, noSelectivity);
    }
}
