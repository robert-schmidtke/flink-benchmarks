package org.xtreemfs.flink.benchmark;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class XtreemFSFlinkBenchmark {

	private final Options options;
	private final CommandLineParser parser;
	private final HelpFormatter formatter;

	private final Map<String, AbstractBenchmark> benchmarks;

	private XtreemFSFlinkBenchmark() {
		benchmarks = new HashMap<String, AbstractBenchmark>();

		AbstractBenchmark tpch1 = new TPCH1Benchmark();
		benchmarks.put(tpch1.getName(), tpch1);
		AbstractBenchmark tpch16 = new TPCH16Benchmark();
		benchmarks.put(tpch16.getName(), tpch16);

		options = new Options();
		options.addOption(new Option("h", "help", false, "Print this message."));

		StringBuilder benchmarkNames = new StringBuilder();
		for (Entry<String, AbstractBenchmark> benchmark : benchmarks.entrySet()) {
			benchmarkNames.append("\n- ").append(benchmark.getKey());
			benchmark.getValue().getOptions(options);
		}
		options.addOption(new Option("b", "benchmark", true,
				"Name of the benchmark to execute. Valid options:"
						+ benchmarkNames.toString()));

		parser = new GnuParser();
		formatter = new HelpFormatter();
	}

	private void execute(String[] args) throws ParseException {
		CommandLine cmd = parser.parse(options, args);

		if (cmd.hasOption("h")) {
			printUsage();
			return;
		}

		if (!cmd.hasOption("benchmark")) {
			throw new ParseException("Missing required argument -b,--benchmark");
		}

		String benchmarkName = cmd.getOptionValue("benchmark");
		AbstractBenchmark benchmark = benchmarks.get(benchmarkName);
		if (benchmark == null) {
			throw new ParseException("Invalid benchmark: " + benchmarkName);
		}

		try {
			benchmark.configureWithCli(cmd);
		} catch (Throwable t) {
			throw new RuntimeException("Error configuring benchmark: "
					+ t.getMessage(), t);
		}

		try {
			benchmark.execute();
		} catch (Throwable t) {
			throw new RuntimeException("Error during benchmark execution: "
					+ t.getMessage(), t);
		}
	}

	private void printUsage() {
		formatter
				.printHelp(
						"$FLINK_HOME/bin/flink run -m yarn-cluster -yn <N> xtreemfs-flink-benchmark-1.0-SNAPSHOT.jar",
						options, true);
	}

	public static void main(String[] args) {
		XtreemFSFlinkBenchmark benchmark = new XtreemFSFlinkBenchmark();
		try {
			benchmark.execute(args);
		} catch (ParseException e) {
			System.err.println("Error parsing arguments: " + e.getMessage()
					+ ".\n");
			benchmark.printUsage();
			System.exit(1);
		} catch (IllegalArgumentException e) {
			System.err.println("Invalid argument: " + e.getMessage());
			benchmark.printUsage();
			System.exit(1);
		} catch (Throwable t) {
			System.err.println("Other error: " + t.getMessage() + ".\n");
			System.exit(1);
		}
	}

}
