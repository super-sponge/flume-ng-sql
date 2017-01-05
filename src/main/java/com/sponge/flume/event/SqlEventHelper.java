package com.sponge.flume.event;

import org.apache.commons.io.HexDump;
import org.apache.flume.Event;
import org.apache.flume.event.EventHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by sponge on 2016/12/26 0026.
 */
public class SqlEventHelper {
/*
    private static final String HEXDUMP_OFFSET = "00000000";
    private static final String EOL = System.getProperty("line.separator", "\n");
    private static final int DEFAULT_MAX_BYTES = 16;


    private static final Logger LOGGER = LoggerFactory
            .getLogger(SqlEventHelper.class);

    public static String dumpEvent(Event event) {
        return dumpEvent(event, DEFAULT_MAX_BYTES);
    }

    public static String dumpEvent(Event event, int maxBytes) {
        StringBuilder buffer = new StringBuilder();
        if (event == null || event.getBody() == null) {
            buffer.append("null");
        } else if (event.getBody().length == 0) {
            // do nothing... in this case, HexDump.dump() will throw an exception
        } else {
            byte[] body = event.getBody();
            byte[] data = Arrays.copyOf(body, Math.min(body.length, maxBytes));
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                HexDump.dump(data, 0, out, 0);
                String hexDump = new String(out.toByteArray());
                // remove offset since it's not relevant for such a small dataset
                if(hexDump.startsWith(HEXDUMP_OFFSET)) {
                    hexDump = hexDump.substring(HEXDUMP_OFFSET.length());
                }
                buffer.append(hexDump);
            } catch (Exception e) {
                if(LOGGER.isInfoEnabled()) {
                    LOGGER.info("Exception while dumping event", e);
                }
                buffer.append("...Exception while dumping: ").append(e.getMessage());
            }
            String result = buffer.toString();
            if(result.endsWith(EOL) && buffer.length() > EOL.length()) {
                buffer.delete(buffer.length() - EOL.length(), buffer.length()).toString();
            }
        }
        return "{ headers:" + event.getHeaders() + " body:" + buffer + " }";
    }

    public static String dumpSqlEvent(Event event)  {
        String body = Bytes.toString(event.getBody());


        return "{ headers:" + event.getHeaders() + " body:" + body + " }";
    }

    public static String[] dumpSqlEventList(Event event) {
        String body = Bytes.toString(event.getBody());
        String[] results = body.split("\",\"");
        return  results;
    }
*/
    public static void main(String[] args){
        String body= "\"379\",\"263\",\"I\",\"N\",\"WSBFD";
//        body = body.substring(1);
//        body = body.substring(0, body.length() -1);
        body = trimFirstAndLastChar(body,'\"');
        String[] results = body.split("\",\"");

        for(int i = 0; i < results.length; i ++) {
            System.out.println(results[i]);
        }

    }

    public static String trimFirstAndLastChar(String source,char element){
        boolean beginIndexFlag = true;
        boolean endIndexFlag = true;
        do{
            int beginIndex = source.indexOf(element) == 0 ? 1 : 0;
            int endIndex = source.lastIndexOf(element) + 1 == source.length() ? source.lastIndexOf(element) : source.length();
            source = source.substring(beginIndex, endIndex);
            beginIndexFlag = (source.indexOf(element) == 0);
            endIndexFlag = (source.lastIndexOf(element) + 1 == source.length());
        } while (beginIndexFlag || endIndexFlag);
        return source;
    }
}

