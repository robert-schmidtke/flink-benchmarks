package org.xtreemfs.flink.benchmark;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;

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
			fileSizes = BenchmarkUtil.copyFiles(dbgenExecutable.getParentFile()
					.getAbsolutePath(), dfsWorkingDirectory.getAbsolutePath(),
					"lineitem.tbl");
			copyFilesMillis = System.currentTimeMillis() - copyFilesMillis;
		} catch (IOException e) {
			throw new RuntimeException("Error during file copy: "
					+ e.getMessage(), e);
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
							String.class, String.class, String.class);

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

			mappedLineItems
					.groupBy(0, 1)
					.sortGroup(0, Order.ASCENDING)
					.sortGroup(1, Order.ASCENDING)
					.reduceGroup(
							new GroupReduceFunction<Tuple8<String, String, Float, Float, Float, Float, Float, Long>, Tuple10<String, String, Float, Float, Float, Float, Float, Float, Float, Long>>() {

								private static final long serialVersionUID = -5628953612923330341L;

								@Override
								public void reduce(
										Iterable<Tuple8<String, String, Float, Float, Float, Float, Float, Long>> tuples,
										Collector<Tuple10<String, String, Float, Float, Float, Float, Float, Float, Float, Long>> collector)
										throws Exception {
									Tuple10<String, String, Float, Float, Float, Float, Float, Float, Float, Long> result = new Tuple10<String, String, Float, Float, Float, Float, Float, Float, Float, Long>(
											"", "", 0.0f, 0.0f, 0.0f, 0.0f,
											0.0f, 0.0f, 0.0f, 0L);
									Iterator<Tuple8<String, String, Float, Float, Float, Float, Float, Long>> it = tuples
											.iterator();
									while (it.hasNext()) {
										Tuple8<String, String, Float, Float, Float, Float, Float, Long> tuple = it
												.next();
										result.f0 = tuple.f0;
										result.f1 = tuple.f1;
										result.f2 += tuple.f2;
										result.f3 += tuple.f3;
										result.f4 += tuple.f4;
										result.f5 += tuple.f5;
										result.f6 += tuple.f6;
										result.f9 += tuple.f7;
									}
									result.f8 = result.f6 / result.f9;
									result.f7 = result.f3 / result.f9;
									result.f6 = result.f2 / result.f9;
									collector.collect(result);
								}
							}).print();

			// SortPartitionOperator<Tuple8<String, String, Float, Float, Float,
			// Float, Float, Long>> result = mappedLineItems
			// .groupBy(0, 1).sum(2).andSum(3).andSum(4).andSum(5)
			// .andSum(6).andSum(7).sortPartition(0, Order.ASCENDING)
			// .sortPartition(1, Order.ASCENDING);
			//
			// result.map(
			// new MapFunction<Tuple8<String, String, Float, Float, Float,
			// Float, Float, Long>, Tuple10<String, String, Float, Float, Float,
			// Float, Float, Float, Float, Long>>() {
			//
			// private static final long serialVersionUID =
			// -6596407623350163628L;
			//
			// @Override
			// public Tuple10<String, String, Float, Float, Float, Float, Float,
			// Float, Float, Long> map(
			// Tuple8<String, String, Float, Float, Float, Float, Float, Long>
			// tuple)
			// throws Exception {
			// return new Tuple10<String, String, Float, Float, Float, Float,
			// Float, Float, Float, Long>(
			// tuple.f0, tuple.f1, tuple.f2, tuple.f3,
			// tuple.f4, tuple.f5, tuple.f2 / tuple.f7,
			// tuple.f3 / tuple.f7, tuple.f6 / tuple.f7,
			// tuple.f7);
			// }
			// }).print();

			jobExecResult = env.getLastJobExecutionResult();

			// env.execute("TPC-H Query 1");
		} catch (Exception e) {
			throw new RuntimeException("Error during execution: "
					+ e.getMessage(), e);
		}

		jobMillis = System.currentTimeMillis() - jobMillis;

		long deleteFilesMillis = System.currentTimeMillis();
		BenchmarkUtil.deleteFiles(dfsWorkingDirectory.getAbsolutePath(),
				"lineitem.tbl");
		deleteFilesMillis = System.currentTimeMillis() - deleteFilesMillis;

		System.out.println("dbgen: " + dbgenMillis + "ms, copyFiles: "
				+ copyFilesMillis + "ms, job (wall): " + jobMillis
				+ "ms, job (flink): " + jobExecResult.getNetRuntime()
				+ "ms deleteFiles: " + deleteFilesMillis + "ms, fileSizes: "
				+ fileSizes);
	}

	@Override
	public String getName() {
		return "tpch1";
	}

}
