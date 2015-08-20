package com.vulcan.sau.util.log4j;

import org.apache.log4j.Level;
import org.apache.log4j.Priority;

public class PerfLevel extends Level {
    private static final long serialVersionUID = 1l;

	public static final int PERF_INT = Priority.INFO_INT -1;
	public static final String PERF_STR = "PERF";
	
	public static final PerfLevel PERF = new PerfLevel(PERF_INT,PERF_STR,6);
	
	protected PerfLevel(int level, String strLevel, int sysLogEquiv) {
		super(level, strLevel, sysLogEquiv);

	}
	
	
	public static Level toLevel(String sArg) {
		return (Level)toLevel(sArg,PerfLevel.PERF);
	}
	
	
	public static Level toLevel(String sArg,Level defaultValue) {
		
		if(sArg == null) {
			return defaultValue;
		}
		
		String strVal = sArg.toUpperCase();
		
		if(strVal.equals(PerfLevel.PERF_STR)) {
			return PerfLevel.PERF;
		}
		else {
			return toLevel(sArg);
		}
	}

	public static Level toLevel(int iLevel) throws IllegalArgumentException {
		if(iLevel == PerfLevel.PERF_INT) {
			return PerfLevel.PERF;
		}
		else {
			return toLevel(iLevel);
		}
			
	}
}
