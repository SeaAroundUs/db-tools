package com.vulcan.sau.util.sql;

import org.apache.log4j.Logger;

import com.vulcan.sau.util.timers.Timer;
import com.vulcan.sau.util.timers.TimerFactory;

public abstract class SqlThread extends Thread {
    protected static final Logger logger = Logger.getLogger(SqlThread.class);
    
    private boolean idling = false;
    private int threadId;

    protected SqlThread(int threadId) {
    	this.threadId = threadId;
    	setName("Thread " + threadId);
    }

    public boolean isIdling() {
        return idling;
    }
       
    protected void setIdling(boolean idleState) {
        idling = idleState;
    }
    
    protected int getThreadId() {
        return threadId;
    }
    
    abstract public boolean isFinished();
    
    abstract public String getTimerName();
    
    abstract protected void internalRun() throws Exception;
    
    abstract public void shutdown();
    
    public void run() {
        Timer timer = TimerFactory.getTimer(logger, getTimerName(), TimerFactory.DEFAULT_TIMER);
            
        logger.debug("Starting thread " + threadId + "... ");
        
        timer.start();
                    
        try {
            internalRun();
        }
        catch (Exception e) {
            logger.error("Thread " + threadId + ": Skipping command with error: ", e );
        }
        finally {
            timer.stop();
        }
    }
}
