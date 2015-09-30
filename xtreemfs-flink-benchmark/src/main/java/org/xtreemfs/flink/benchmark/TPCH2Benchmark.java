package org.xtreemfs.flink.benchmark;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;

public class TPCH2Benchmark extends AbstractBenchmark {

	@Override
	public void execute() {
		if (noJob) {
			return;
		}

		JobExecutionResult jobExecResult = null;
		long jobMillis = -1L;
		try {
			ExecutionEnvironment env = ExecutionEnvironment
					.getExecutionEnvironment();

			Configuration parameters = new Configuration();

			List<CsvReader> partReaders = new ArrayList<CsvReader>();
			List<CsvReader> supplierReaders = new ArrayList<CsvReader>();
			List<CsvReader> partSuppReaders = new ArrayList<CsvReader>();

			if (inputChunks == 0) {
				// include the following fields
				// 0: p_partkey, identifier
				// 2: p_mfgr, 25-char string (fixed)
				// 4: p_type, 25-char string (fixed)
				// 5: p_size, integer
				partReaders.add(env
						.readCsvFile(dfsWorkingDirectoryUri + "part.tbl")
						.fieldDelimiter("|").includeFields(0x43));

				// include all fields
				// 0: s_suppkey, identifier
				// 1: s_name, 25-char string (fixed)
				// 2: s_address, 40-char string (variable)
				// 3: s_nationkey, identifier
				// 4: s_phone, 15-char string (fixed)
				// 5: s_acctbal, floating point
				// 6: s_comment, 101-char string (variable)
				supplierReaders.add(env
						.readCsvFile(dfsWorkingDirectoryUri + "supplier.tbl")
						.fieldDelimiter("|").includeFields(0x127));

				// include the following fields
				// 0: ps_partkey, identifier
				// 1: ps_suppkey, identifier
				// 3: ps_supplycost, floating point
				partSuppReaders.add(env
						.readCsvFile(dfsWorkingDirectoryUri + "partsupp.tbl")
						.fieldDelimiter("|").includeFields(0x13));
			} else {
				for (int i = 0; i < inputChunks; ++i) {
					partReaders.add(env
							.readCsvFile(
									dfsWorkingDirectoryUri + "part.tbl." + i)
							.fieldDelimiter("|").includeFields(0x43));

					supplierReaders.add(env
							.readCsvFile(
									dfsWorkingDirectoryUri + "supplier.tbl."
											+ i).fieldDelimiter("|")
							.includeFields(0x127));

					partSuppReaders.add(env
							.readCsvFile(
									dfsWorkingDirectoryUri + "partsupp.tbl."
											+ i).fieldDelimiter("|")
							.includeFields(0x13));
				}
			}

			// include first three fields.
			// 0: n_nationkey, identifier
			// 1: n_name, 25-char string (fixed)
			// 2: n_regionkey, identifier
			CsvReader nationReader = env
					.readCsvFile(dfsWorkingDirectoryUri + "nation.tbl")
					.fieldDelimiter("|").includeFields(0x7);

			// include first two fields.
			// 0: r_regionkey, identifier
			// 1: r_name, 25-char string (fixed)
			CsvReader regionReader = env
					.readCsvFile(dfsWorkingDirectoryUri + "region.tbl")
					.fieldDelimiter("|").includeFields(0x3);

			DataSet<Tuple4<Integer, String, String, Integer>> parts = partReaders
					.get(0)
					.types(Integer.class, String.class, String.class,
							Integer.class).withParameters(parameters);
			DataSet<Tuple7<Integer, String, String, Integer, String, Float, String>> suppliers = supplierReaders
					.get(0)
					.types(Integer.class, String.class, String.class,
							Integer.class, String.class, Float.class,
							String.class).withParameters(parameters);
			DataSet<Tuple3<Integer, Integer, Float>> partSupps = partSuppReaders
					.get(0).types(Integer.class, Integer.class, Float.class)
					.withParameters(parameters);
			for (int i = 1; i < inputChunks; ++i) {
				parts = parts.union(partReaders
						.get(i)
						.types(Integer.class, String.class, String.class,
								Integer.class).withParameters(parameters));
				suppliers = suppliers.union(supplierReaders
						.get(i)
						.types(Integer.class, String.class, String.class,
								Integer.class, String.class, Float.class,
								String.class).withParameters(parameters));
				partSupps = partSupps.union(partSuppReaders.get(i)
						.types(Integer.class, Integer.class, Float.class)
						.withParameters(parameters));
			}

			DataSet<Tuple3<Integer, String, Integer>> nations = nationReader
					.types(Integer.class, String.class, Integer.class)
					.withParameters(parameters);
			DataSet<Tuple2<Integer, String>> regions = regionReader.types(
					Integer.class, String.class).withParameters(parameters);

			// return first 100 rows
			// select
			// s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone,
			// s_comment
			// from
			// part, supplier, partsupp, nation, region
			// where
			// p_partkey = ps_partkey
			// and s_suppkey = ps_suppkey
			// and p_size = [SIZE]
			// and p_type like '%[TYPE]'
			// and s_nationkey = n_nationkey
			// and n_regionkey = r_regionkey
			// and r_name = '[REGION]'
			// and ps_supplycost = (
			// select
			// min(ps_supplycost)
			// from
			// partsupp, supplier, nation, region
			// where
			// p_partkey = ps_partkey
			// and s_suppkey = ps_suppkey
			// and s_nationkey = n_nationkey
			// and n_regionkey = r_regionkey
			// and r_name = '[REGION]'
			// )
			// order by
			// s_acctbal desc,
			// n_name,
			// s_name,
			// p_partkey;

			// validation values
			final int size = 15;
			final String type = "BRASS";
			final String region = "EUROPE";

			DataSet<Tuple4<Integer, String, String, Integer>> filteredParts = parts
					.filter(new FilterFunction<Tuple4<Integer, String, String, Integer>>() {

						private static final long serialVersionUID = -4244882561875016474L;

						@Override
						public boolean filter(
								Tuple4<Integer, String, String, Integer> value)
								throws Exception {
							// p_type like '%[TYPE]'
							return value.f2.endsWith(type)
							// p_size = [SIZE]
									&& value.f3.equals(size);
						}

					});

			DataSet<Tuple2<Integer, String>> filteredRegions = regions
					.filter(new FilterFunction<Tuple2<Integer, String>>() {

						private static final long serialVersionUID = -6694451768512421827L;

						@Override
						public boolean filter(Tuple2<Integer, String> value)
								throws Exception {
							// r_name = '[REGION]'
							return value.f1.equals(region);
						}

					});

			DataSet<Tuple3<Integer, String, Integer>> filteredNations = nations
					.join(filteredRegions)
					// n_regionkey
					.where(2)
					// = r_regionkey
					.equalTo(0)
					.map(new MapFunction<Tuple2<Tuple3<Integer, String, Integer>, Tuple2<Integer, String>>, Tuple3<Integer, String, Integer>>() {

						private static final long serialVersionUID = 5084686318023821336L;

						@Override
						public Tuple3<Integer, String, Integer> map(
								Tuple2<Tuple3<Integer, String, Integer>, Tuple2<Integer, String>> value)
								throws Exception {
							return value.f0;
						}

					});

			DataSet<Tuple7<Integer, String, String, Integer, String, Float, String>> filteredSuppliers = suppliers
					.join(filteredNations)
					// s_nationkey
					.where(3)
					// = n_nationkey
					.equalTo(0)
					.map(new MapFunction<Tuple2<Tuple7<Integer, String, String, Integer, String, Float, String>, Tuple3<Integer, String, Integer>>, Tuple7<Integer, String, String, Integer, String, Float, String>>() {

						private static final long serialVersionUID = 3160369939656631987L;

						@Override
						public Tuple7<Integer, String, String, Integer, String, Float, String> map(
								Tuple2<Tuple7<Integer, String, String, Integer, String, Float, String>, Tuple3<Integer, String, Integer>> value)
								throws Exception {
							return value.f0;
						}

					});

			// partSupp, part, supplier
			DataSet<Tuple3<Tuple3<Integer, Integer, Float>, Tuple4<Integer, String, String, Integer>, Tuple7<Integer, String, String, Integer, String, Float, String>>> result = filteredParts
					.join(partSupps)
					// p_partkey
					.where(0)
					// = ps_partkey
					.equalTo(0)
					// s_suppkey = ps_suppkey
					.join(filteredSuppliers.join(partSupps).where(0).equalTo(1))
					// join on the now duplicated partSupp
					.where(1)
					.equalTo(1)
					.map(new MapFunction<Tuple2<Tuple2<Tuple4<Integer, String, String, Integer>, Tuple3<Integer, Integer, Float>>, Tuple2<Tuple7<Integer, String, String, Integer, String, Float, String>, Tuple3<Integer, Integer, Float>>>, Tuple3<Tuple3<Integer, Integer, Float>, Tuple4<Integer, String, String, Integer>, Tuple7<Integer, String, String, Integer, String, Float, String>>>() {

						private static final long serialVersionUID = 4768350882055831546L;

						@Override
						public Tuple3<Tuple3<Integer, Integer, Float>, Tuple4<Integer, String, String, Integer>, Tuple7<Integer, String, String, Integer, String, Float, String>> map(
								Tuple2<Tuple2<Tuple4<Integer, String, String, Integer>, Tuple3<Integer, Integer, Float>>, Tuple2<Tuple7<Integer, String, String, Integer, String, Float, String>, Tuple3<Integer, Integer, Float>>> value)
								throws Exception {
							return new Tuple3<Tuple3<Integer, Integer, Float>, Tuple4<Integer, String, String, Integer>, Tuple7<Integer, String, String, Integer, String, Float, String>>(
									value.f0.f1, value.f0.f0, value.f1.f0);
						}

					});

			// TODO ordering
			// TODO (limit to 100 here to reduce cost of next step?)
			// TODO get min supply cost

			result.first(100).writeAsCsv(dfsWorkingDirectoryUri + "tpchq2.csv",
					"\n", "|", WriteMode.OVERWRITE);

			jobMillis = System.currentTimeMillis();
			env.execute();
			jobMillis = System.currentTimeMillis() - jobMillis;

			copyFromWorkingDirectory(outputDirectory.getAbsolutePath(),
					"tpchq2.csv");

			jobExecResult = env.getLastJobExecutionResult();
		} catch (Exception e) {
			throw new RuntimeException("Error during execution: "
					+ e.getMessage(), e);
		}

		System.out.println("job (wall): " + jobMillis + "ms, job (flink): "
				+ jobExecResult.getNetRuntime() + "ms");
	}

	@Override
	public String getName() {
		return "tpch2";
	}

}
