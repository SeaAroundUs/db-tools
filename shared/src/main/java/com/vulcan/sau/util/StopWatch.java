package com.vulcan.sau.util;

public class StopWatch {
	private long startTime = 0;
	private long stopTime = 0;
	private boolean running = false;
	
	
	public void start() {
	    this.startTime = System.currentTimeMillis();
	    this.running = true;
	}
	
	
	public void stop() {
	    this.stopTime = System.currentTimeMillis();
	    this.running = false;
	}
	
	
	//elaspsed time in milliseconds
	public long getElapsedTime() {
	    long elapsed;
	    if (running) {
	         elapsed = (System.currentTimeMillis() - startTime);
	    }
	    else {
	        elapsed = (stopTime - startTime);
	    }
	    return elapsed;
	}
	
	
	//elaspsed time in seconds
	public long getElapsedTimeSecs() {
	    long elapsed;
	    if (running) {
	        elapsed = ((System.currentTimeMillis() - startTime) / 1000);
	    }
	    else {
	        elapsed = ((stopTime - startTime) / 1000);
	    }
	    return elapsed;
	}

	// Get elapsed time in seconds
	public int getElapsedClockSec() {
	    int elapsed;
	    if (running) {
	        elapsed = (int)((System.currentTimeMillis() - startTime) / 1000) % 60;
	    }
	    else {
	        elapsed = (int)((stopTime - startTime) / 1000) % 60;
	    }
	    return elapsed;
	}
	
	// Get elapsed time in minutes
	public int getElapsedClockMin() {
	    int elapsed;
	    if (running) {
	        elapsed = (int)((System.currentTimeMillis() - startTime) / (60*1000)) % 60;
	    }
	    else {
	        elapsed = (int)((stopTime - startTime) / (60*1000)) % 60;
	    }
	    return elapsed;
	}
	
	// Get elapsed time in hours
	public int getElapsedClockHour() {
	    int elapsed;
	    if (running) {
	        elapsed = (int)((System.currentTimeMillis() - startTime) / (60*60*1000)) % 24;
	    }
	    else {
	        elapsed = (int)((stopTime - startTime) / (60*60*1000)) % 24;
	    }
	    return elapsed;
	}

	// Get elapsed time in days
	public int getElapsedClockDay() {
	    int elapsed;
	    if (running) {
	        elapsed = (int)((System.currentTimeMillis() - startTime) / (24*60*60*1000));
	    }
	    else {
	        elapsed = (int)((stopTime - startTime) / (24*60*60*1000));
	    }
	    return elapsed;
	}
	
}


