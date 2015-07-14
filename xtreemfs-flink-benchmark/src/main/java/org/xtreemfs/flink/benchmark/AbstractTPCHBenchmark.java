package org.xtreemfs.flink.benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public abstract class AbstractTPCHBenchmark extends AbstractBenchmark {

	private static final String OPTION_TPCH_DBGEN_EXECUTABLE = "tpch-dbgen-executable";
	private static final String OPTION_TPCH_DBGEN_SCALE = "tpch-dbgen-scale";

	protected File dbgenExecutable;
	private float dbgenScale;

	@Override
	public void getOptions(Options options) {
		super.getOptions(options);

		options.addOption(new Option(null, OPTION_TPCH_DBGEN_EXECUTABLE, true,
				"Path to the compiled DBGen."));
		options.addOption(new Option(
				null,
				OPTION_TPCH_DBGEN_SCALE,
				true,
				"Scale of the database population. Scale 1.0 represents ~1 GB of data (defaults to 1.0)."));
	}

	@Override
	public void configureWithCli(CommandLine cmd) {
		super.configureWithCli(cmd);

		if (!cmd.hasOption(OPTION_TPCH_DBGEN_SCALE)) {
			dbgenScale = 1.0f;
		} else {
			dbgenScale = Float.parseFloat(cmd
					.getOptionValue(OPTION_TPCH_DBGEN_SCALE));
		}

		if (!cmd.hasOption(OPTION_TPCH_DBGEN_EXECUTABLE)) {
			throw new IllegalArgumentException("Missing required argument --"
					+ OPTION_TPCH_DBGEN_EXECUTABLE);
		}
		dbgenExecutable = new File(
				cmd.getOptionValue(OPTION_TPCH_DBGEN_EXECUTABLE));
		if (!dbgenExecutable.exists()) {
			throw new IllegalArgumentException("DBGen executable "
					+ dbgenExecutable.getPath() + " does not exist");
		}
	}

	protected void dbgen() throws IOException, InterruptedException {
		Process dbgen = new ProcessBuilder(dbgenExecutable.getAbsolutePath(),
				"-v", "-f", "-s", Float.toString(dbgenScale)).directory(
				dbgenExecutable.getParentFile()).start();

		BufferedReader errorReader = new BufferedReader(new InputStreamReader(
				dbgen.getErrorStream()));
		StringBuilder errors = new StringBuilder();
		String line;
		while ((line = errorReader.readLine()) != null) {
			errors.append(line);
		}
		errorReader.close();

		if (dbgen.waitFor() != 0) {
			throw new RuntimeException("Non-zero DBGen exit status, stderr:\n"
					+ errors.toString());
		}
	}

}
