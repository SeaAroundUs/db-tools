
package com.vulcan.sau.util.timers;

import org.apache.log4j.Logger;

import com.vulcan.sau.util.log4j.PerfLevel;

class IterationPerfTimer implements Timer {

    private String key;
    private Logger logger;

    private long curStart;
    private long total;

    /**
     * @param logger --
     *                the logger that PerfTimer will use to log results.
     * @param key --
     *                the key that will is the name associated with the time value.
     */

    IterationPerfTimer(Logger logger, String key) {
        this.key = key;
        this.logger = logger;
    }

    /**
     * start timing this iteration
     */
    public void start() {
        this.curStart = System.currentTimeMillis();
    }

    /**
     * stop timing the current iteration.
     * 
     * @throws Exception
     *                 if stop() called after stop() or reset().
     */
    public void stop() {
        if (this.curStart == Timer.UNINITIALIZED) {
            throw new IllegalArgumentException("must call stop after start.");
        }
        this.total += (System.currentTimeMillis() - this.curStart);
        this.curStart = Timer.UNINITIALIZED;
    }

    /**
     * add values and flush to log.
     */
    public void reset() {
        logger.log(PerfLevel.PERF, this.key + Timer.DELIMITER + this.total);
        this.total = 0;
    }
    
    /**
     * 
     * @return current elapsed time.
     */
    
    public long getTotal() {
        return total;
    }
}
