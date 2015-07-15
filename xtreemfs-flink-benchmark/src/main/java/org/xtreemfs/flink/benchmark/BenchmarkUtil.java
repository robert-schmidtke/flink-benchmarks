package org.xtreemfs.flink.benchmark;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class BenchmarkUtil {

	public static long copyFiles(String fromDir, String toDir, String... files)
			throws IOException {
		if (!fromDir.endsWith(File.separator)) {
			fromDir += File.separator;
		}

		if (!toDir.endsWith(File.separator)) {
			toDir += File.separator;
		}

		long bytesCopied = 0;
		for (String file : files) {
			File from = new File(fromDir + file);
			bytesCopied += from.length();
			FileUtils.copyFile(from, new File(toDir + file));
		}
		return bytesCopied;
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
