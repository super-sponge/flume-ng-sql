package com.sponge.flume.sink;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.DeleteRequest;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * A sql serializer to be used with the AsyncHBaseSink
 * that returns puts from an event, by writing the event
 * body into it. The headers are discarded. It also updates a row in hbase
 * which acts as an event counter.
 * <p>
 * Takes optional parameters:<p>
 * <tt>rowPrefix:</tt> The prefix to be used. Default: <i>default</i><p>
 * <tt>incrementRow</tt> The row to increment. Default: <i>incRow</i><p>
 * <tt>suffix:</tt> <i>uuid/random/timestamp.</i>Default: <i>uuid</i><p>
 * <p>
 * Mandatory parameters: <p>
 * <tt>cf:</tt>Column family.<p>
 * Components that have no defaults and will not be used if absent:
 * <tt>payloadColumn:</tt> Which column to put payload in. If it is not present,
 * event data will not be written.<p>
 * <tt>incrementColumn:</tt> Which column to increment. If this is absent, it
 * means no column is incremented.
 */
public class SqlAsyncHbaseEventSerializer implements AsyncHbaseEventSerializer {
    private byte[] table;
    private byte[] cf;
    private byte[] payload;
    private byte[] incrementColumn;
    private byte[] incrementRow;


    private byte[][] colMaps;
    private int cols;

    private final static String delimiter = "\",\"";
    private final static int ROWKEY_INDEX = 2;
    private final static int OP_TYPE=1;



    private static final Logger logger =
            LoggerFactory.getLogger(SqlAsyncHbaseEventSerializer.class);

    @Override
    public void initialize(byte[] table, byte[] cf) {
        this.table = table;
        this.cf = cf;
    }

    @Override
    public List<PutRequest> getActions() {
        List<PutRequest> actions = new ArrayList<PutRequest>();

        try {

            String body = trimFirstAndLastChar(Bytes.toString(payload), '\"');
            String[] values = body.split(delimiter);

            //seq,priv,type,dat1,dat2
            if ("I".equals(values[OP_TYPE]) || "U".equals(values[OP_TYPE])) {
                int copydata = Math.min(values.length -3, cols);
                for (int i = 0; i < copydata; i++) {
                    PutRequest putRequest = new PutRequest(table, values[ROWKEY_INDEX].getBytes(Charsets.UTF_8), cf,
                            colMaps[i], values[i + 3].getBytes(Charsets.UTF_8));
                    actions.add(putRequest);
                }
            }

        } catch (Exception e) {
            throw new FlumeException("Could not get row key!", e);
        }

        return actions;
    }

    public List<DeleteRequest> getActionsDelete() {
        List<DeleteRequest> actions = new ArrayList<DeleteRequest>();

        try {

            String body = trimFirstAndLastChar(Bytes.toString(payload), '\"');
            String[] values = body.split(delimiter);

            //seq,priv,type,dat1,dat2
            if ("D".equals(values[OP_TYPE])) {
                DeleteRequest deleteRequest = new DeleteRequest(table,
                values[ROWKEY_INDEX].getBytes(Charsets.UTF_8), cf);
                actions.add(deleteRequest);
            }

        } catch (Exception e) {
            throw new FlumeException("Could not get row key!", e);
        }

        return actions;
    }

    public List<AtomicIncrementRequest> getIncrements() {
        List<AtomicIncrementRequest> actions = new ArrayList<AtomicIncrementRequest>();
        if (incrementColumn != null) {
            AtomicIncrementRequest inc = new AtomicIncrementRequest(table,
                    incrementRow, cf, incrementColumn);
            actions.add(inc);
        }
        return actions;
    }

    @Override
    public void cleanUp() {
        // TODO Auto-generated method stub

    }

    @Override
    public void configure(Context context) {
        String iCol = context.getString("incrementColumn", "iCol");

        if (iCol != null && !iCol.isEmpty()) {
            incrementColumn = iCol.getBytes(Charsets.UTF_8);
        }
        incrementRow = context.getString("incrementRow", "incRow").getBytes(Charsets.UTF_8);

        //get column maps
        String colmap = context.getString("colmaps", "col1");
        String[] strcolMaps = colmap.split(",");
        logger.info("colMaps is : " + colmap);
        cols = strcolMaps.length;
        colMaps = new byte[cols][];
        for (int i = 0; i < cols; i++) {
            colMaps[i] = strcolMaps[i].getBytes(Charsets.UTF_8);
        }

    }

    @Override
    public void setEvent(Event event) {
        this.payload = event.getBody();
    }

    @Override
    public void configure(ComponentConfiguration conf) {
        // TODO Auto-generated method stub
    }

    public static String trimFirstAndLastChar(String source,char element){
//        boolean beginIndexFlag = true;
//        boolean endIndexFlag = true;
//        do{
//            int beginIndex = source.indexOf(element) == 0 ? 1 : 0;
//            int endIndex = source.lastIndexOf(element) + 1 == source.length() ? source.lastIndexOf(element) : source.length();
//            source = source.substring(beginIndex, endIndex);
//            beginIndexFlag = (source.indexOf(element) == 0);
//            endIndexFlag = (source.lastIndexOf(element) + 1 == source.length());
//        } while (beginIndexFlag || endIndexFlag);

        return source.substring(1, source.length()- 2);
    }

}
