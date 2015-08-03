package org.xtreemfs.flink.benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public abstract class AbstractBenchmark {

	public static enum DFSType {
		HDFS, XTREEMFS;
	}

	private DFSType dfsType;

	// General options.
	private static final String OPTION_DFS_WORKING_DIRECTORY_URI = "dfs-working-directory-uri";
	protected String dfsWorkingDirectoryUri;
	private static final String OPTION_FLINK_ASSIGN_LOCALLY_ONLY = "flink-assign-locally-only";
	protected boolean flinkAssignLocallyOnly;
	private static final String OPTION_NO_JOB = "no-job";
	protected boolean noJob;
	protected static final String OPTION_OUTPUT_DIRECTORY_PATH = "output-directory-path";
	protected File outputDirectory;

	// Options needed when using HDFS.
	private static final String OPTION_HDFS_BLOCKSIZE = "hdfs-blocksize";
	private String hdfsBlocksize;
	private static final String OPTION_HDFS_HADOOP_EXECUTABLE = "hdfs-hadoop-executable";
	private File hdfsHadoopExecutable;
	private static final String OPTION_HDFS_REPLICATION = "hdfs-replication";
	private int hdfsReplication;

	// Options needed when using XtreemFS.
	private static final String OPTION_XTREEMFS_WORKING_DIRECTORY_PATH = "xtreemfs-working-directory-path";
	private File xtreemfsWorkingDirectory;

	protected AbstractBenchmark() {
	}

	public void configureWithCli(CommandLine cmd) {
		if (!cmd.hasOption(OPTION_DFS_WORKING_DIRECTORY_URI)) {
			throw new IllegalArgumentException("Missing required argument --"
					+ OPTION_DFS_WORKING_DIRECTORY_URI);
		}
		dfsWorkingDirectoryUri = cmd
				.getOptionValue(OPTION_DFS_WORKING_DIRECTORY_URI);
		if (!dfsWorkingDirectoryUri.endsWith("/")) {
			dfsWorkingDirectoryUri += "/";
		}

		URI uri;
		try {
			uri = new URI(dfsWorkingDirectoryUri);
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException(
					"Invalid distributed file system URI: "
							+ dfsWorkingDirectoryUri);
		}

		try {
			dfsType = DFSType.valueOf(uri.getScheme().toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(
					"Unsupported distributed file system scheme: "
							+ uri.getScheme());
		}

		switch (dfsType) {
		case HDFS:
			if (!cmd.hasOption(OPTION_HDFS_HADOOP_EXECUTABLE)) {
				throw new IllegalArgumentException(
						"Missing required argument --"
								+ OPTION_HDFS_HADOOP_EXECUTABLE);
			}
			hdfsHadoopExecutable = new File(
					cmd.getOptionValue(OPTION_HDFS_HADOOP_EXECUTABLE));
			if (!hdfsHadoopExecutable.exists()) {
				throw new IllegalArgumentException("HDFS Hadoop executable "
						+ hdfsHadoopExecutable.getPath() + " does not exist");
			}

			if (!cmd.hasOption(OPTION_HDFS_BLOCKSIZE)) {
				hdfsBlocksize = "128M";
			} else {
				hdfsBlocksize = cmd.getOptionValue(OPTION_HDFS_BLOCKSIZE);
				if (!Pattern.matches("^[1-9]+[kKmMgGtTpPeE]?$", hdfsBlocksize)) {
					throw new IllegalArgumentException("Bad argument for --"
							+ OPTION_HDFS_BLOCKSIZE + ": " + hdfsBlocksize);
				}
			}

			if (!cmd.hasOption(OPTION_HDFS_REPLICATION)) {
				hdfsReplication = 3;
			} else {
				try {
					hdfsReplication = Integer.parseInt(cmd
							.getOptionValue(OPTION_HDFS_REPLICATION));
					if (hdfsReplication <= 0) {
						throw new IllegalArgumentException("--"
								+ OPTION_HDFS_REPLICATION + " must be positive");
					}
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Bad argument for --"
							+ OPTION_HDFS_REPLICATION + ": "
							+ cmd.getOptionValue(OPTION_HDFS_REPLICATION));
				}
			}
			break;
		case XTREEMFS:
			if (!cmd.hasOption(OPTION_XTREEMFS_WORKING_DIRECTORY_PATH)) {
				throw new IllegalArgumentException(
						"Missing required argument --"
								+ OPTION_XTREEMFS_WORKING_DIRECTORY_PATH);
			}
			xtreemfsWorkingDirectory = new File(
					cmd.getOptionValue(OPTION_XTREEMFS_WORKING_DIRECTORY_PATH));
			if (!xtreemfsWorkingDirectory.exists()) {
				throw new IllegalArgumentException(
						"Mounted XtreemFS working directory "
								+ xtreemfsWorkingDirectory.getPath()
								+ " does not exist");
			}
			break;
		}

		flinkAssignLocallyOnly = cmd
				.hasOption(OPTION_FLINK_ASSIGN_LOCALLY_ONLY);
		noJob = cmd.hasOption(OPTION_NO_JOB);

		if (!cmd.hasOption(OPTION_OUTPUT_DIRECTORY_PATH)) {
			throw new IllegalArgumentException("Missing required arguments --"
					+ OPTION_OUTPUT_DIRECTORY_PATH);
		}
		outputDirectory = new File(
				cmd.getOptionValue(OPTION_OUTPUT_DIRECTORY_PATH));
		if (!outputDirectory.exists()) {
			throw new IllegalArgumentException("Output directory "
					+ outputDirectory.getPath() + " does not exist");
		}
	}

	public void getOptions(Options options) {
		options.addOption(new Option(null, OPTION_DFS_WORKING_DIRECTORY_URI,
				true,
				"URI of the working directory hosting the distributed file system."));
		options.addOption(new Option(null, OPTION_FLINK_ASSIGN_LOCALLY_ONLY,
				false,
				"Specify if only local splits should be assigned. Disabled by default."));
		options.addOption(new Option(null, OPTION_NO_JOB, false,
				"Specify if no job should be executed. Disabled by default."));
		options.addOption(new Option(null, OPTION_OUTPUT_DIRECTORY_PATH, true,
				"Path of the output directory where results are placed."));

		options.addOption(new Option(
				null,
				OPTION_HDFS_BLOCKSIZE,
				true,
				"Block size to use in bytes, or as multiples with case-insensitive suffixes K, M, G, T, P, E (HDFS only). Defaults to 128M."));
		options.addOption(new Option(null, OPTION_HDFS_HADOOP_EXECUTABLE, true,
				"Path of the Hadoop executable (HDFS only)"));
		options.addOption(new Option(null, OPTION_HDFS_REPLICATION, true,
				"Replication factor to use, must be positive (HDFS only). Defaults to 1."));

		options.addOption(new Option(null,
				OPTION_XTREEMFS_WORKING_DIRECTORY_PATH, true,
				"Path of the mounted working directory (XtreemFS only)."));
	}

	protected long copyToWorkingDirectory(String fromDir, String... files)
			throws IOException {
		long fileSizes = 0L;
		switch (dfsType) {
		case HDFS:
			if (!fromDir.endsWith(File.separator)) {
				fromDir += File.separator;
			}

			List<String> hadoopCommand = new ArrayList<String>();
			hadoopCommand.add(hdfsHadoopExecutable.getAbsolutePath());
			hadoopCommand.add("fs");
			hadoopCommand.add("-Ddfs.blocksize=" + hdfsBlocksize);
			hadoopCommand.add("-Ddfs.replication=" + hdfsReplication);
			hadoopCommand.add("-copyFromLocal");
			hadoopCommand.add("-f");
			hadoopCommand.add("");
			hadoopCommand.add(dfsWorkingDirectoryUri);
			for (String file : files) {
				hadoopCommand.set(6, fromDir + file);
				Process hadoop = new ProcessBuilder(hadoopCommand).start();
				BufferedReader errorReader = new BufferedReader(
						new InputStreamReader(hadoop.getErrorStream()));
				StringBuilder errors = new StringBuilder();
				String line;
				while ((line = errorReader.readLine()) != null) {
					errors.append(line);
				}
				errorReader.close();

				try {
					if (hadoop.waitFor() != 0) {
						throw new RuntimeException(
								"Non-zero Hadoop exit status, stderr:\n"
										+ errors.toString());
					}
				} catch (InterruptedException e) {
					throw new RuntimeException(
							"Error during Hadoop copyFromLocal: "
									+ e.getMessage(), e);
				}

				fileSizes += new File(fromDir + file).length();
			}
			break;
		case XTREEMFS:
			fileSizes = BenchmarkUtil.copyFiles(fromDir,
					xtreemfsWorkingDirectory.getAbsolutePath(), files);
		}
		return fileSizes;
	}

	protected long copyFromWorkingDirectory(String toDir, String... files)
			throws IOException {
		long fileSizes = 0L;
		switch (dfsType) {
		case HDFS:
			if (!toDir.endsWith(File.separator)) {
				toDir += File.separator;
			}

			List<String> hadoopCommand = new ArrayList<String>();
			hadoopCommand.add(hdfsHadoopExecutable.getAbsolutePath());
			hadoopCommand.add("fs");
			hadoopCommand.add("-copyToLocal");
			hadoopCommand.add("");
			hadoopCommand.add(toDir);
			for (String file : files) {
				hadoopCommand.set(3, dfsWorkingDirectoryUri + file);
				Process hadoop = new ProcessBuilder(hadoopCommand).start();
				BufferedReader errorReader = new BufferedReader(
						new InputStreamReader(hadoop.getErrorStream()));
				StringBuilder errors = new StringBuilder();
				String line;
				while ((line = errorReader.readLine()) != null) {
					errors.append(line);
				}
				errorReader.close();

				try {
					if (hadoop.waitFor() != 0) {
						throw new RuntimeException(
								"Non-zero Hadoop exit status, stderr:\n"
										+ errors.toString());
					}
				} catch (InterruptedException e) {
					throw new RuntimeException(
							"Error during Hadoop copyToLocal: "
									+ e.getMessage(), e);
				}

				fileSizes += new File(toDir + file).length();
			}
			break;
		case XTREEMFS:
			BenchmarkUtil.copyFiles(xtreemfsWorkingDirectory.getAbsolutePath(),
					toDir, files);
			break;
		}
		return fileSizes;
	}

	protected void deleteFromWorkingDirectory(String... files)
			throws IOException {
		switch (dfsType) {
		case HDFS:
			List<String> hadoopCommand = new ArrayList<String>();
			hadoopCommand.add(hdfsHadoopExecutable.getAbsolutePath());
			hadoopCommand.add("fs");
			hadoopCommand.add("-rm");
			hadoopCommand.add("-f");
			hadoopCommand.add("");
			for (String file : files) {
				hadoopCommand.set(4, dfsWorkingDirectoryUri + file);
				Process hadoop = new ProcessBuilder(hadoopCommand).start();
				BufferedReader errorReader = new BufferedReader(
						new InputStreamReader(hadoop.getErrorStream()));
				StringBuilder errors = new StringBuilder();
				String line;
				while ((line = errorReader.readLine()) != null) {
					errors.append(line);
				}
				errorReader.close();

				try {
					if (hadoop.waitFor() != 0) {
						throw new RuntimeException(
								"Non-zero Hadoop exit status, stderr:\n"
										+ errors.toString());
					}
				} catch (InterruptedException e) {
					throw new RuntimeException("Error during Hadoop rm: "
							+ e.getMessage(), e);
				}
			}
			break;
		case XTREEMFS:
			BenchmarkUtil.deleteFiles(
					xtreemfsWorkingDirectory.getAbsolutePath(), files);
			break;
		}
	}

	public abstract void execute();

	public abstract String getName();

}
