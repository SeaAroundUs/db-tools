package com.vulcan.sau.util;

import java.io.File;
import java.io.IOException;

public class WebGet {
//	final private static String windowsWgetPath = "C:\\Program Files\\GnuWin32\\bin\\wget.exe"; 
	final private static String windowsWgetPath = "D:\\GnuWin32\\bin\\wget.exe"; 
	final private static String linuxWgetPath = "/usr/bin/wget";
	
	public static boolean FetchAndSaveWebPage(String outputFileName, String pageUrl) throws IOException, InterruptedException {
		ensureDirectoryExists(outputFileName);
		
		ProcessBuilder pb;
		
		if (System.getProperty("os.name").startsWith("Windows"))
			pb = new ProcessBuilder(new File(windowsWgetPath).getCanonicalPath(), "-O", outputFileName, pageUrl);
		else
			pb = new ProcessBuilder(new File(linuxWgetPath).getCanonicalPath(), "-O", outputFileName, pageUrl);

		ProcessUtil runner = new ProcessUtil(pb);
		runner.join(20000);
		String output = runner.getOutput();

		if (runner.isAlive()) {
			throw new RuntimeException("Page get timed out: " + output);
		} else if (runner.getException() != null) {
			throw runner.getException();
		} else if (runner.getStatus() != 0) {
			System.out.println("Exit status = " + runner.getStatus());
			System.out.println("Err: " + output);
			throw new RuntimeException("Page get for " + outputFileName + " failed):  " + output);
		}

		return true;
	}
	
	private static void ensureDirectoryExists (String fileName)
	{
		File file = new File(fileName.substring(0, fileName.lastIndexOf(File.separator)));
		file.mkdirs();
	}
}
