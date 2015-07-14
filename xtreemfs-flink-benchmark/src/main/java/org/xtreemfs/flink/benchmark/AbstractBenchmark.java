package org.xtreemfs.flink.benchmark;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public abstract class AbstractBenchmark {

	private final String OPTION_DFS_WORKING_DIRECTORY_PATH = "dfs-working-directory-path";
	private final String OPTION_DFS_WORKING_DIRECTORY_URI = "dfs-working-directory-uri";

	protected File dfsWorkingDirectory;
	protected String dfsWorkingDirectoryUri;

	protected AbstractBenchmark() {
	}

	public void configureWithCli(CommandLine cmd) {
		if (!cmd.hasOption(OPTION_DFS_WORKING_DIRECTORY_PATH)) {
			throw new IllegalArgumentException("Missing required argument --"
					+ OPTION_DFS_WORKING_DIRECTORY_PATH);
		}
		dfsWorkingDirectory = new File(
				cmd.getOptionValue(OPTION_DFS_WORKING_DIRECTORY_PATH));
		if (!dfsWorkingDirectory.exists()) {
			throw new IllegalArgumentException(
					"Distributed file system working directory "
							+ dfsWorkingDirectory.getPath() + " does not exist");
		}

		if (!cmd.hasOption(OPTION_DFS_WORKING_DIRECTORY_URI)) {
			throw new IllegalArgumentException("Missing required argument --"
					+ OPTION_DFS_WORKING_DIRECTORY_URI);
		}
		dfsWorkingDirectoryUri = cmd.getOptionValue(OPTION_DFS_WORKING_DIRECTORY_URI);
		
		try {
			new URI(dfsWorkingDirectoryUri);
		} catch(URISyntaxException e) {
			throw new IllegalArgumentException("Invalid distributed file system URI: " + dfsWorkingDirectoryUri);
		}
	}

	public void getOptions(Options options) {
		options.addOption(new Option(null, OPTION_DFS_WORKING_DIRECTORY_PATH,
				true,
				"Path of the working directory hosting the distributed file system."));
		options.addOption(new Option(null, OPTION_DFS_WORKING_DIRECTORY_URI,
				true,
				"URI of the working directory hosting the distributed file system."));
	}

	public abstract void execute();

	public abstract String getName();

}
