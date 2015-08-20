
package com.vulcan.sau.util.timers;

import org.apache.log4j.Logger;

import com.vulcan.sau.util.log4j.PerfLevel;

/**
 * PerfTimer is used to log timing statistics. Usage: (1) create a PerfTimer object passing in a valid Logger object and a key that any time value will be associated with. (2) call start() to start
 * timing. (3) call stop() to stop timing and log the elapsed time with the key provided at object creation. NOTE: calling stop() w/o calling start() previously will result in an illegal state
 * exception being thrown.
 */

class PerfTimer implements Timer {

    private Logger logger;
    private long startTime;
    private long stopTime;
    private String key;

    /**
     * @param logger --
     *            the logger that PerfTimer will use to log results.
     * @param key --
     *            the key that will is the name associated with the time value.
     */
    PerfTimer(Logger logger, String key) {
        this.logger = logger;
        this.key = key;
    }

    /**
     * start tracking time.
     */

    public void start() {
        startTime = System.currentTimeMillis();
        stopTime = Timer.UNINITIALIZED;
    }

    /**
     * stop tracking time and log result using the PERF log level.
     * 
     * @throws Exception
     *             if stop is called without calling start.
     */

    public void stop() {

        if (this.startTime == Timer.UNINITIALIZED) {
            throw new IllegalStateException("cannot call stop() without calling start() first");
        }

        stopTime = System.currentTimeMillis();
        logger.log(PerfLevel.PERF, this.key + Timer.DELIMITER + (stopTime - this.startTime));
        startTime = Timer.UNINITIALIZED;
    }

    /**
     * this is a no-op, as such throw an exception
     */
    public void reset() {
        throw new UnsupportedOperationException("reset is not implemented for PerfTimer");
    }

    public long getTotal() {
        return stopTime - startTime;
    }

}
