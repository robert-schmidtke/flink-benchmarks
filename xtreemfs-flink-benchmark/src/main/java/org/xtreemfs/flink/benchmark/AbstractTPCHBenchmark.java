package org.xtreemfs.flink.benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public abstract class AbstractTPCHBenchmark extends AbstractBenchmark {

	private static final String OPTION_TPCH_DBGEN_EXECUTABLE = "tpch-dbgen-executable";
	protected File dbgenExecutable;

	private static final String OPTION_TPCH_DBGEN_REGENERATE = "tpch-dbgen-regenerate";
	private boolean dbgenRegenerate;

	private static final String OPTION_TPCH_DBGEN_SCALE = "tpch-dbgen-scale";
	private float dbgenScale;

	@Override
	public void getOptions(Options options) {
		super.getOptions(options);

		options.addOption(new Option(null, OPTION_TPCH_DBGEN_EXECUTABLE, true,
				"Path to the compiled DBGen."));
		options.addOption(new Option(
				null,
				OPTION_TPCH_DBGEN_REGENERATE,
				false,
				"Specify if data should be regenerated if it's already there. Oisabled by default."));
		options.addOption(new Option(
				null,
				OPTION_TPCH_DBGEN_SCALE,
				true,
				"Scale of the database population. Scale 1.0 represents ~1 GB of data. Defaults to 1.0."));
	}

	@Override
	public void configureWithCli(CommandLine cmd) {
		super.configureWithCli(cmd);

		if (!cmd.hasOption(OPTION_TPCH_DBGEN_SCALE)) {
			dbgenScale = 1.0f;
		} else {
			try {
				dbgenScale = Float.parseFloat(cmd
						.getOptionValue(OPTION_TPCH_DBGEN_SCALE));
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Bad argument for --"
						+ OPTION_TPCH_DBGEN_SCALE + ": "
						+ cmd.getOptionValue(OPTION_TPCH_DBGEN_SCALE));
			}
		}

		dbgenRegenerate = cmd.hasOption(OPTION_TPCH_DBGEN_REGENERATE);

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
		String tblBasePath = dbgenExecutable.getParentFile().getAbsolutePath();
		if (!tblBasePath.endsWith(File.separator)) {
			tblBasePath += File.separator;
		}

		boolean allTablesExist = new File(tblBasePath + "customer.tbl")
				.exists();
		allTablesExist &= new File(tblBasePath + "lineitem.tbl").exists();
		allTablesExist &= new File(tblBasePath + "nation.tbl").exists();
		allTablesExist &= new File(tblBasePath + "orders.tbl").exists();
		allTablesExist &= new File(tblBasePath + "partsupp.tbl").exists();
		allTablesExist &= new File(tblBasePath + "part.tbl").exists();
		allTablesExist &= new File(tblBasePath + "region.tbl").exists();
		allTablesExist &= new File(tblBasePath + "supplier.tbl").exists();

		if (allTablesExist && !dbgenRegenerate) {
			return;
		}

		List<String> dbgenCommand = new ArrayList<String>();
		dbgenCommand.add(dbgenExecutable.getAbsolutePath());
		dbgenCommand.add("-v");
		dbgenCommand.add("-f");
		dbgenCommand.add("-s");
		dbgenCommand.add(Float.toString(dbgenScale));

		Process dbgen = new ProcessBuilder(dbgenCommand).directory(
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
