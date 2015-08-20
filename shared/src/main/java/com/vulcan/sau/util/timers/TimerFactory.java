package com.vulcan.sau.util.timers;

import org.apache.log4j.Logger;

public class TimerFactory {

	public static final int DEFAULT_TIMER = 1;
	public static final int ITERATION_TIMER = 2;
	
	/**
	 * get timers here.
	 * @param timerType
	 * @return Timer
	 * @throws IllegalArgumentException
	 */
	public static Timer getTimer(Logger logger, String key,int timerType) throws IllegalArgumentException {
		
		Timer timer = null;
		
		switch(timerType) {
		case DEFAULT_TIMER:
			timer =  new PerfTimer(logger,key);
		break;
		case ITERATION_TIMER:
			timer = new IterationPerfTimer(logger,key);
		break;
		}
		
		
		return timer;
	}
}
