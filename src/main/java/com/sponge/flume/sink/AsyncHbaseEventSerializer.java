package com.sponge.flume.sink;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.DeleteRequest;
import org.hbase.async.PutRequest;

import java.util.List;

/**
 * Created by sponge on 2016/12/26 0026.
 */
public interface AsyncHbaseEventSerializer extends Configurable,
        ConfigurableComponent {

    /**
     * Initialize the event serializer.
     * @param table - The table the serializer should use when creating
     * {@link org.hbase.async.PutRequest} or
     * {@link org.hbase.async.AtomicIncrementRequest}.
     * @param cf - The column family to be used.
     */
    public void initialize(byte[] table, byte[] cf);

    /**
     * @param Event to be written to HBase.
     */
    public void setEvent(Event event);

    /**
     * Get the actions that should be written out to hbase as a result of this
     * event. This list is written to hbase.
     * @return List of {@link org.hbase.async.PutRequest} which
     * are written as such to HBase.
     *
     *
     */
    public List<PutRequest> getActions();

    public List<DeleteRequest> getActionsDelete();

    /**
     * Get the increments that should be made in hbase as a result of this
     * event. This list is written to hbase.
     * @return List of {@link org.hbase.async.AtomicIncrementRequest} which
     * are written as such to HBase.
     *
     *
     */
    public List<AtomicIncrementRequest> getIncrements();

    /**
     * Clean up any state. This will be called when the sink is being stopped.
     */
    public void cleanUp();
}

