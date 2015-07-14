package org.xtreemfs.flink.benchmark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public abstract class AbstractBenchmark {

	protected AbstractBenchmark() {
	}

	public void configureWithCli(CommandLine cmd) {
	}

	public void getOptions(Options options) {
	}

	public abstract void execute();

	public abstract String getName();

}
