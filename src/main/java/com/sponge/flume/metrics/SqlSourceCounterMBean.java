package com.sponge.flume.metrics;

/**
 * Created by sponge on 2016/12/26 0026.
 */
public interface SqlSourceCounterMBean {
    public long getEventCount();
    public void incrementEventCount(int value);
    public long getAverageThroughput();
    public long getCurrentThroughput();
    public long getMaxThroughput();
}