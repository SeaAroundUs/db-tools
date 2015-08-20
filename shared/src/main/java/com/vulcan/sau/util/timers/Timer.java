package com.vulcan.sau.util.timers;

public interface Timer {

	public static final String DELIMITER = ":";
	public static final int UNINITIALIZED = -1;
	
	public void start();
	public void stop();
	public void reset();
	public long getTotal();
        
}
