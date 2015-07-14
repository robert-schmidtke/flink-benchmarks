package org.xtreemfs.flink.benchmark;

public class TPCH1Benchmark extends AbstractTPCHBenchmark {

	public void execute() {
		try {
			dbgen();
		} catch (Throwable t) {
			throw new RuntimeException("Error during DBgen: " + t.getMessage(), t);
		}
		
		// TODO move data from dbgenExecutable.getParentFile() to XtreemFS, measure the time
		// TODO run the actual queries
	}

	@Override
	public String getName() {
		return "tpch1";
	}

}
