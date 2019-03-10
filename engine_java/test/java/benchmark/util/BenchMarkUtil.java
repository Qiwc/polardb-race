package benchmark.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;

public class BenchMarkUtil {
	public static String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	public static int VALUE_BUFFER_SIZE = 1024 * 1024;
	public static int VALUE_COUNT = 256;
	
	public static byte[] getRandomValue() {
		StringBuilder str = new StringBuilder();
		int strLen = ALPHABET.length();
		Random random = new Random(System.nanoTime());
		for (int i = 0; i < VALUE_BUFFER_SIZE; i++) {
			str.append(ALPHABET.charAt(random.nextInt(strLen)));
		}
		return str.toString().getBytes(StandardCharsets.UTF_8);
	}
	
	public static byte[] getRandomKey(int len, Random random) {
		byte[] bs = new byte[len];
		random.nextBytes(bs);
		return bs;
	}
	
	public static long getDBPathSize(String path) {
		return FileUtils.sizeOfDirectory(new File(path)) / (1024 * 1024);
	}
	
	public static void clearDBPath(String path) {
		try {
			Process process = Runtime.getRuntime().exec("cd " + path + " && rm -rf *");
			process.getOutputStream().close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void dropPageCache() {
		try {
			Process process = Runtime.getRuntime().exec("echo 3 >/proc/sys/vm/drop_caches");
			process.getOutputStream().close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
