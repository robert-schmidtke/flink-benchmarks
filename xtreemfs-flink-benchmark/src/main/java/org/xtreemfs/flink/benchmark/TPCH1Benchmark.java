package org.xtreemfs.flink.benchmark;

import java.io.IOException;

public class TPCH1Benchmark extends AbstractTPCHBenchmark {

	public void execute() {
		try {
			dbgen();
		} catch (Throwable t) {
			throw new RuntimeException("Error during DBgen: " + t.getMessage(), t);
		}
		
		try {
			BenchmarkUtil.copyFiles(dbgenExecutable.getParentFile().getAbsolutePath(), dfsWorkingDirectory.getAbsolutePath(), "lineitem.tbl");
		} catch (IOException e) {
			throw new RuntimeException("Error during file copy: " + e.getMessage(), e);
		}
		
		// TODO run the actual queries
		
		BenchmarkUtil.deleteFiles(dfsWorkingDirectory.getAbsolutePath(), "lineitem.tbl");
	}

	@Override
	public String getName() {
		return "tpch1";
	}

}
