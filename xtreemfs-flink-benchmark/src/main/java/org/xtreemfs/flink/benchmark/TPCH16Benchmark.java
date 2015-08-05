package org.xtreemfs.flink.benchmark;

import java.io.IOException;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;

public class TPCH16Benchmark extends AbstractTPCHBenchmark {

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
					.getAbsolutePath(), "part.tbl", "partsupp.tbl",
					"supplier.tbl");
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

		JobExecutionResult jobExecResult = null;
		long jobMillis = -1L;
		try {
			ExecutionEnvironment env = ExecutionEnvironment
					.getExecutionEnvironment();

			Configuration parameters = new Configuration();
			parameters.setBoolean(FileInputFormat.ASSIGN_LOCALLY_ONLY_FLAG,
					flinkAssignLocallyOnly);

			// Include first field, skip next two fields, include next three
			// fields (111001 in binary).
			// 0: p_partkey, identifier
			// 3: p_brand, 10-char string (fixed)
			// 4: p_type, 25-char string (variable)
			// 5: p_size, integer
			CsvReader partReader = env
					.readCsvFile(dfsWorkingDirectoryUri + "part.tbl")
					.fieldDelimiter("|").includeFields(0x39);

			// Include first two fields (11 in binary).
			// 0: ps_partkey, identifier (foreign key to p_partkey)
			// 1: ps_suppkey, identifier (foreign key to s_suppkey)
			CsvReader partSuppReader = env
					.readCsvFile(dfsWorkingDirectoryUri + "partsupp.tbl")
					.fieldDelimiter("|").includeFields(0x2);

			// Include first field, skip next five fields, include next field
			// (1000001 in binary).
			// 0: s_suppkey, identifier
			// 6: s_comment, 101-char string (variable)
			CsvReader supplierReader = env
					.readCsvFile(dfsWorkingDirectoryUri + "supplier.tbl")
					.fieldDelimiter("|").includeFields(0x41);

			DataSource<Tuple4<Integer, String, String, Integer>> parts = partReader
					.types(Integer.class, String.class, String.class,
							Integer.class);
			DataSource<Tuple2<Integer, Integer>> partSupps = partSuppReader
					.types(Integer.class, Integer.class);
			DataSource<Tuple2<Integer, String>> suppliers = supplierReader
					.types(Integer.class, String.class);

			// select
			// p_brand, p_type, p_size, count(distinct ps_suppkey) as
			// supplier_cnt
			// from
			// partsupp, part
			// where
			// p_partkey = ps_partkey
			// and p_brand <> '[BRAND]'
			// and p_type not like '[TYPE]%'
			// and p_size in ([SIZE1], [SIZE2], [SIZE3], [SIZE4], [SIZE5],
			// [SIZE6], [SIZE7], [SIZE8])
			// and ps_suppkey not in (
			// select
			// s_suppkey
			// from
			// supplier
			// where
			// s_comment like '%Customer%Complaints%'
			// )
			// group by
			// p_brand, p_type, p_size
			// order by
			// supplier_cnt desc, p_brand, p_type, p_size;

			// Validation values
			// BRAND = Brand#45
			final String brand = "Brand#45";
			// TYPE = MEDIUM POLISHED
			final String type = "MEDIUM POLISHED";
			// SIZE1 = 49
			// SIZE2 = 14
			// SIZE3 = 23
			// SIZE4 = 45
			// SIZE5 = 19
			// SIZE6 = 3
			// SIZE7 = 36
			// SIZE8 = 9
			final int[] sizes = new int[] { 49, 14, 23, 45, 19, 3, 36, 9 };

			DataSet<Tuple4<Integer, String, String, Integer>> filteredParts = parts
					.filter(new FilterFunction<Tuple4<Integer, String, String, Integer>>() {

						private static final long serialVersionUID = 4094592217462752183L;

						@Override
						public boolean filter(
								Tuple4<Integer, String, String, Integer> value)
								throws Exception {
							// p_partkey, p_brand, p_type, p_size
							boolean retain = false;
							if (!value.f1.equals(brand)
									&& !value.f2.startsWith(type)) {
								for (int i = 0; i < sizes.length && !retain; ++i) {
									retain = value.f3.equals(sizes[i]);
								}
							}
							return retain;
						}

					});

			DataSet<Tuple2<Integer, String>> filteredSuppliers = suppliers
					.filter(new FilterFunction<Tuple2<Integer, String>>() {

						private static final long serialVersionUID = 3037162901890348112L;

						@Override
						public boolean filter(Tuple2<Integer, String> value)
								throws Exception {
							// s_suppkey, s_comment
							int customerOffset = value.f1.indexOf("Customer");
							if (customerOffset >= 0) {
								return value.f1.indexOf("Complaints") < customerOffset + 8;
							} else {
								return true;
							}
						}

					});

			DataSet<Tuple2<Integer, Integer>> filteredPartSupps = partSupps
					.join(filteredSuppliers)
					.where(0)
					.equalTo(0)
					.map(new MapFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, String>>, Tuple2<Integer, Integer>>() {
						private static final long serialVersionUID = 7459347408951603520L;

						@Override
						public Tuple2<Integer, Integer> map(
								Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, String>> value)
								throws Exception {
							// (ps_partkey, ps_suppkey), (s_suppkey, s_comment)
							return value.f0;
						}

					});

			DataSet<Tuple4<String, String, Integer, Long>> joinedParts = filteredParts
					.join(filteredPartSupps)
					.where(0)
					.equalTo(0)
					.map(new MapFunction<Tuple2<Tuple4<Integer, String, String, Integer>, Tuple2<Integer, Integer>>, Tuple4<String, String, Integer, Long>>() {

						private static final long serialVersionUID = -3416648871054977236L;

						@Override
						public Tuple4<String, String, Integer, Long> map(
								Tuple2<Tuple4<Integer, String, String, Integer>, Tuple2<Integer, Integer>> value)
								throws Exception {
							// (p_partkey, p_brand, p_type, p_size),
							// (ps_partkey, ps_suppkey)
							return new Tuple4<String, String, Integer, Long>(
									value.f0.f1, value.f0.f2, value.f0.f3, 1L);
						}

					});

			DataSet<Tuple4<String, String, Integer, Long>> result = joinedParts
					.groupBy(0, 1, 2).sum(3).sortPartition(3, Order.DESCENDING)
					.setParallelism(1).sortPartition(0, Order.ASCENDING)
					.setParallelism(1).sortPartition(1, Order.ASCENDING)
					.setParallelism(1).sortPartition(2, Order.ASCENDING)
					.setParallelism(1);

			result.writeAsCsv(dfsWorkingDirectoryUri + "tpchq16.csv", "\n",
					"|", WriteMode.OVERWRITE);

			jobMillis = System.currentTimeMillis();
			env.execute();
			jobMillis = System.currentTimeMillis() - jobMillis;

			// TODO check result

			copyFilesMillis -= System.currentTimeMillis();
			fileSizes += copyFromWorkingDirectory(
					outputDirectory.getAbsolutePath(), "tpchq16.csv");
			copyFilesMillis += System.currentTimeMillis();

			jobExecResult = env.getLastJobExecutionResult();
		} catch (Exception e) {
			throw new RuntimeException("Error during execution: "
					+ e.getMessage(), e);
		}

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
			deleteFromWorkingDirectory("part.tbl", "partsupp.tbl",
					"supplier.tbl");
		} catch (IOException e) {
			throw new RuntimeException("Error during file remove: "
					+ e.getMessage(), e);
		}
		return System.currentTimeMillis() - deleteFilesMillis;
	}

	@Override
	public String getName() {
		return "tpch16";
	}

}