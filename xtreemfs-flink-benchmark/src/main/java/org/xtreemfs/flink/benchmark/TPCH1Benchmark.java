package org.xtreemfs.flink.benchmark;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;

public class TPCH1Benchmark extends AbstractTPCHBenchmark {

	@Override
	public void execute() {
		long dbgenMillis = System.currentTimeMillis();
		try {
			dbgen();
			dbgenMillis = System.currentTimeMillis() - dbgenMillis;
		} catch (Throwable t) {
			throw new RuntimeException("Error during DBgen: " + t.getMessage(),
					t);
		}

		long copyFilesMillis = System.currentTimeMillis();
		long fileSizes;
		try {
			fileSizes = copyToWorkingDirectory(dbgenExecutable.getParentFile()
					.getAbsolutePath(), "lineitem.tbl");
			copyFilesMillis = System.currentTimeMillis() - copyFilesMillis;
		} catch (IOException e) {
			throw new RuntimeException("Error during file copy: "
					+ e.getMessage(), e);
		}

		if (noJob) {
			long deleteFilesMillis = cleanup();
			System.out.println("dbgen: " + dbgenMillis + "ms, copyFiles: "
					+ copyFilesMillis + "ms, deleteFiles: " + deleteFilesMillis
					+ "ms, fileSizes: " + fileSizes);
			return;
		}

		// select
		// l_returnflag,
		// l_linestatus,
		// sum(l_quantity) as sum_qty,
		// sum(l_extendedprice) as sum_base_price,
		// sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
		// sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
		// avg(l_quantity) as avg_qty,
		// avg(l_extendedprice) as avg_price,
		// avg(l_discount) as avg_disc,
		// count(*) as count_order
		// from
		// lineitem
		// where
		// l_shipdate <= date '1998-12-01' - interval ':1' day (3)
		// group by
		// l_returnflag,
		// l_linestatus
		// order by
		// l_returnflag,
		// l_linestatus;

		JobExecutionResult jobExecResult = null;
		long jobMillis = System.currentTimeMillis();
		try {
			ExecutionEnvironment env = ExecutionEnvironment
					.getExecutionEnvironment();

			Configuration parameters = new Configuration();
			parameters.setBoolean(FileInputFormat.ASSIGN_LOCALLY_ONLY_FLAG,
					flinkAssignLocallyOnly);

			CsvReader reader = env.readCsvFile(
					dfsWorkingDirectoryUri + "lineitem.tbl")
					.fieldDelimiter("|");

			// 11111110000 in binary, indicate to skip the first four fields and
			// include the following seven fields.
			// 4: l_quantity, decimal
			// 5: l_extendedprice, decimal
			// 6: l_discount, decimal
			// 7: l_tax, decimal
			// 8: l_returnflag, 1-char string
			// 9: l_linestatus, 1-char string
			// 10: l_shipdate, date
			reader.includeFields(0x7F0);

			final SimpleDateFormat dateParser = new SimpleDateFormat(
					"yyyy-MM-dd");
			final long referenceDate = dateParser.parse("1998-12-01").getTime();

			// 86400000 milliseconds in a day, 90 is the TPC-H Q1 validation
			// value.
			final long delta = 90 * 86400000; // (new Random().nextInt(61) + 60)
												// * 86400000;

			DataSource<Tuple7<Float, Float, Float, Float, String, String, String>> lineItems = reader
					.types(Float.class, Float.class, Float.class, Float.class,
							String.class, String.class, String.class)
					.withParameters(parameters);

			// Filter on date.
			DataSet<Tuple7<Float, Float, Float, Float, String, String, String>> filteredLineItems = lineItems
					.filter(new FilterFunction<Tuple7<Float, Float, Float, Float, String, String, String>>() {

						private static final long serialVersionUID = -818134663949093125L;

						@Override
						public boolean filter(
								Tuple7<Float, Float, Float, Float, String, String, String> tuple)
								throws Exception {
							Date date = dateParser.parse(tuple.f6);
							return date.getTime() <= referenceDate - delta;
						}
					});

			// Map for calculations.
			DataSet<Tuple8<String, String, Float, Float, Float, Float, Float, Long>> mappedLineItems = filteredLineItems
					.map(new MapFunction<Tuple7<Float, Float, Float, Float, String, String, String>, Tuple8<String, String, Float, Float, Float, Float, Float, Long>>() {

						private static final long serialVersionUID = 8021849053433646399L;

						@Override
						public Tuple8<String, String, Float, Float, Float, Float, Float, Long> map(
								Tuple7<Float, Float, Float, Float, String, String, String> tuple)
								throws Exception {
							float discountedPrice = tuple.f1
									* (1.0f - tuple.f2);
							return new Tuple8<String, String, Float, Float, Float, Float, Float, Long>(
									tuple.f4, tuple.f5, tuple.f0, tuple.f1,
									discountedPrice, discountedPrice
											* (1.0f + tuple.f3), tuple.f2, 1L);
						}
					});

			DataSet<Tuple10<String, String, Float, Float, Float, Float, Float, Float, Float, Long>> result = mappedLineItems
					.groupBy(0, 1)
					.sum(2)
					.andSum(3)
					.andSum(4)
					.andSum(5)
					.andSum(6)
					.andSum(7)
					.map(new MapFunction<Tuple8<String, String, Float, Float, Float, Float, Float, Long>, Tuple10<String, String, Float, Float, Float, Float, Float, Float, Float, Long>>() {

						private static final long serialVersionUID = -6596407623350163628L;

						@Override
						public Tuple10<String, String, Float, Float, Float, Float, Float, Float, Float, Long> map(
								Tuple8<String, String, Float, Float, Float, Float, Float, Long> tuple)
								throws Exception {
							return new Tuple10<String, String, Float, Float, Float, Float, Float, Float, Float, Long>(
									tuple.f0, tuple.f1, tuple.f2, tuple.f3,
									tuple.f4, tuple.f5, tuple.f2 / tuple.f7,
									tuple.f3 / tuple.f7, tuple.f6 / tuple.f7,
									tuple.f7);
						}
					}).sortPartition(0, Order.ASCENDING).setParallelism(1)
					.sortPartition(1, Order.ASCENDING).setParallelism(1);
			result.print();

			// Output according to dbgen (factor 1.0):
			// l|l|sum_qty|sum_base_price|sum_disc_price|sum_charge|avg_qty|avg_price|avg_disc|count_order
			// A|F|37734107.00|56586554400.73|53758257134.87|55909065222.83|25.52|38273.13|0.05|1478493
			// N|F|991417.00|1487504710.38|1413082168.05|1469649223.19|25.52|38284.47|0.05|38854
			// N|O|74476040.00|111701729697.74|106118230307.61|110367043872.50|25.50|38249.12|0.05|2920374
			// R|F|37719753.00|56568041380.90|53741292684.60|55889619119.83|25.51|38250.85|0.05|1478870

			// So we cannot test the sums and counts, but we can check the
			// averages and the order.
			List<Tuple10<String, String, Float, Float, Float, Float, Float, Float, Float, Long>> results = result
					.collect();
			if (results.size() != 4) {
				System.out.println("Incorrect number of results: "
						+ results.size() + ", expected 4.");
			} else {
				checkResult(results.get(0), "A", "F", 25.52f, 38273.13f, 0.05f,
						0.002f);
				checkResult(results.get(1), "N", "F", 25.52f, 38284.47f, 0.05f,
						0.002f);
				checkResult(results.get(2), "N", "O", 25.50f, 38249.12f, 0.05f,
						0.002f);
				checkResult(results.get(3), "R", "F", 25.51f, 38250.85f, 0.05f,
						0.002f);
			}

			jobExecResult = env.getLastJobExecutionResult();
		} catch (Exception e) {
			throw new RuntimeException("Error during execution: "
					+ e.getMessage(), e);
		}

		jobMillis = System.currentTimeMillis() - jobMillis;

		long deleteFilesMillis = cleanup();
		System.out.println("dbgen: " + dbgenMillis + "ms, copyFiles: "
				+ copyFilesMillis + "ms, job (wall): " + jobMillis
				+ "ms, job (flink): " + jobExecResult.getNetRuntime()
				+ "ms deleteFiles: " + deleteFilesMillis + "ms, fileSizes: "
				+ fileSizes);
	}

	private long cleanup() {
		long deleteFilesMillis = System.currentTimeMillis();
		try {
			deleteFromWorkingDirectory("lineitem.tbl");
		} catch (IOException e) {
			throw new RuntimeException("Error during file remove: "
					+ e.getMessage(), e);
		}
		return System.currentTimeMillis() - deleteFilesMillis;
	}

	private static void checkResult(
			Tuple10<String, String, Float, Float, Float, Float, Float, Float, Float, Long> result,
			String returnFlag, String lineStatus, float avgQty, float avgPrice,
			float avgDisc, float tolerance) {
		if (!returnFlag.equals(result.f0) || !lineStatus.equals(result.f1)) {
			System.out.print("Element is in the wrong order: " + result.f0
					+ "|" + result.f1 + ", expecting " + returnFlag + "|"
					+ lineStatus);
		} else {
			System.out.print("Order is fine");
		}

		float difference = Math.abs(result.f6 - avgQty) / avgQty;
		if (difference > tolerance) {
			System.out.print("; Avg. Qty. is over tolerance: " + difference
					+ ", expecting " + tolerance);
		} else {
			System.out.print("; Avg. Qty. is fine (" + difference + ")");
		}

		difference = Math.abs(result.f7 - avgPrice) / avgPrice;
		if (difference > tolerance) {
			System.out.print("; Avg. Price is over tolerance: " + difference
					+ ", expecting " + tolerance);
		} else {
			System.out.print("; Avg. Price is fine (" + difference + ")");
		}

		difference = Math.abs(result.f8 - avgDisc) / avgDisc;
		if (difference > tolerance) {
			System.out.println("; Avg. Disc. is over tolerance: " + difference
					+ ", expecting " + tolerance + ".");
		} else {
			System.out.println("; Avg. Disc. is fine (" + difference + ").");
		}
	}

	@Override
	public String getName() {
		return "tpch1";
	}

}
