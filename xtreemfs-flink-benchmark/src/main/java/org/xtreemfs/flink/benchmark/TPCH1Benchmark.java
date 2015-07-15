package org.xtreemfs.flink.benchmark;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple9;

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
		try {
			BenchmarkUtil.copyFiles(dbgenExecutable.getParentFile()
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

			// 86400000 milliseconds in a day.
			final long delta = (new Random().nextInt(61) + 60) * 86400000;

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
			DataSet<Tuple9<String, String, Float, Float, Float, Float, Float, Float, Float>> mappedLineItems = filteredLineItems
					.map(new MapFunction<Tuple7<Float, Float, Float, Float, String, String, String>, Tuple9<String, String, Float, Float, Float, Float, Float, Float, Float>>() {

						private static final long serialVersionUID = 8021849053433646399L;

						@Override
						public Tuple9<String, String, Float, Float, Float, Float, Float, Float, Float> map(
								Tuple7<Float, Float, Float, Float, String, String, String> tuple)
								throws Exception {
							float discountedPrice = tuple.f1
									* (1.0f - tuple.f2);
							return new Tuple9<String, String, Float, Float, Float, Float, Float, Float, Float>(
									tuple.f4, tuple.f5, tuple.f0, tuple.f1,
									discountedPrice, discountedPrice
											* (1.0f + tuple.f3), tuple.f0,
									tuple.f1, tuple.f2);
						}
					});

			DataSet<Tuple9<String, String, Float, Float, Float, Float, Float, Float, Float>> result = mappedLineItems
					.groupBy(0, 1).sum(2).andSum(3).andSum(4).andSum(5)
					.andSum(6).andSum(7).andSum(8)
					.sortPartition(0, Order.ASCENDING)
					.sortPartition(1, Order.ASCENDING);

			final long count = result.count();
			result.map(
					new MapFunction<Tuple9<String, String, Float, Float, Float, Float, Float, Float, Float>, Tuple9<String, String, Float, Float, Float, Float, Float, Float, Float>>() {

						private static final long serialVersionUID = -6596407623350163628L;

						@Override
						public Tuple9<String, String, Float, Float, Float, Float, Float, Float, Float> map(
								Tuple9<String, String, Float, Float, Float, Float, Float, Float, Float> tuple)
								throws Exception {
							tuple.f6 /= count;
							tuple.f7 /= count;
							tuple.f8 /= count;
							return tuple;
						}
					}).print();

			// env.execute("TPC-H Query 1");
		} catch (Exception e) {
			throw new RuntimeException("Error during execution: "
					+ e.getMessage(), e);
		}

		long deleteFilesMillis = System.currentTimeMillis();
		BenchmarkUtil.deleteFiles(dfsWorkingDirectory.getAbsolutePath(),
				"lineitem.tbl");
		deleteFilesMillis = System.currentTimeMillis() - deleteFilesMillis;
	}

	@Override
	public String getName() {
		return "tpch1";
	}

}
