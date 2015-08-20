package com.vulcan.sau.util;

import java.io.InputStream;
import java.io.IOException;

public class ProcessUtil extends Thread {
	private Process process;
	private int status = -1;
	private InterruptedException e;
	StreamGobbler output;

	public ProcessUtil(ProcessBuilder pb) throws IOException {
		pb.redirectErrorStream(true);
		this.process = pb.start();
		this.output = new StreamGobbler(process.getInputStream());
		this.start();
	}

	public void run() {
		try {
			status = process.waitFor();
		} catch (InterruptedException e) {
			this.e = e;
		}
	}

	public Process getProcess() {
		return process;
	}

	public int getStatus() {
		return status;
	}

	public InterruptedException getException() {
		return e;
	}

	public String getOutput() {
		return output.toString();
	}
	
	public void closeStreams() {
		try {
			this.process.getInputStream().close();
			this.process.getOutputStream().close(); 
			this.process.getErrorStream().close();  			
		}
		catch (IOException e) {
		}
	}
	
	public static class StreamGobbler extends Thread {
    	private InputStream in;
    	private StringBuffer output = new StringBuffer();

		public StreamGobbler(InputStream in) {
			this.setDaemon(true);
    		this.in = in;
    		this.start();
    	}
		
		public void run() {
        	try {
            	int c;
				while ((c = in.read()) != -1) {
					output.append((char)c);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		public String toString() {
			return output.toString();
		}
    }
}
