package org.xtreemfs.flink.benchmark;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class BenchmarkUtil {

	public static void copyFiles(String fromDir, String toDir, String... files)
			throws IOException {
		if (!fromDir.endsWith(File.separator)) {
			fromDir += File.separator;
		}

		if (!toDir.endsWith(File.separator)) {
			toDir += File.separator;
		}

		for (String file : files) {
			FileUtils
					.copyFile(new File(fromDir + file), new File(toDir + file));
		}
	}

	public static void deleteFiles(String dir, String... files) {
		if (!dir.endsWith(File.separator)) {
			dir += File.separator;
		}

		for (String file : files) {
			new File(dir + file).delete();
		}
	}
}
