package com.sponge.flume.source;

import com.opencsv.CSVWriter;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import com.sponge.flume.metrics.SqlSourceCounter;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.util.*;

/**
 * A Source to read data from a SQL database. This source ask for new data in a table each configured time.<p>
 * Created by sponge on 2016/12/26 0026.
 *
 * @author <a href="mailto:super_sponge@163.com">liuhb</a>
 */
public class SQLSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger LOG = LoggerFactory.getLogger(SQLSource.class);
    private static final int DEFAULT_BATCH_SIZE = 100;

    private HibernateHelper hibernateHelper;
    private int batchSize;

    Map<String, SQLSourceHelper> mapSQLSourceHelper = new HashMap<String, SQLSourceHelper>();
    Map<String, SqlSourceCounter> mapSqlSourceCounter = new HashMap<String, SqlSourceCounter>();
    Map<String, CSVWriter>  mapCSVWriter = new HashMap<String, CSVWriter>();

    /**
     * Configure the source, load configuration properties and establish connection with database
     */
    @Override
    public void configure(Context context) {

        LOG.getName();

        LOG.info("Reading and processing configuration values for source " + getName());

        batchSize = context.getInteger("batch.size",DEFAULT_BATCH_SIZE);
        //get source configuration
        getMultiSql(context);
        /* Establish connection with database */
        hibernateHelper = new HibernateHelper(context);
        hibernateHelper.establishSession();


    }

    private void getMultiSql(Context context) {

        Map<String, String> tablesProperties = context.getSubProperties("tables.");
        for(Map.Entry<String, String> e : tablesProperties.entrySet()) {
            LOG.info("tableName is {} sql is {}", e.getKey(), e.getValue());
            Context tabContext = context;
            tabContext.put("custom.query", e.getValue());
            mapSQLSourceHelper.put(e.getKey(), new SQLSourceHelper(tabContext, e.getKey()));
            mapSqlSourceCounter.put(e.getKey(), new SqlSourceCounter(e.getKey()));
            mapCSVWriter.put(e.getKey(), new CSVWriter(new ChannelWriter(e.getKey())));
        }
    }

    /**
     * Process a batch of events performing SQL Queries
     */
    @Override
    public Status process() throws EventDeliveryException {

        try {
            for (Map.Entry<String, SQLSourceHelper> entry : mapSQLSourceHelper.entrySet()) {
                String tableName = entry.getKey();
                SQLSourceHelper sqlSourceHelper = entry.getValue();
                hibernateHelper.setSqlSourceHelper(sqlSourceHelper);
                hibernateHelper.setDefaultReadOnly(sqlSourceHelper.isReadOnlySession());


                SqlSourceCounter sqlSourceCounter = mapSqlSourceCounter.get(tableName);
                CSVWriter csvWriter = mapCSVWriter.get(tableName);


                sqlSourceCounter.startProcess();

                List<List<Object>> result = hibernateHelper.executeQuery();

                if (!result.isEmpty()) {
                    csvWriter.writeAll(sqlSourceHelper.getAllRows(result), true);
                    csvWriter.flush();
                    sqlSourceCounter.incrementEventCount(result.size());


                    sqlSourceHelper.updateStatusFile();
                }

                sqlSourceCounter.endProcess(result.size());

                if (result.size() < sqlSourceHelper.getMaxRows()) {
                    Thread.sleep(sqlSourceHelper.getRunQueryDelay());
                }
            }

            return Status.READY;

        } catch (IOException | InterruptedException e) {
            LOG.error("Error procesing row", e);
            return Status.BACKOFF;
        }
    }

    /**
     * Starts the source. Starts the metrics counter.
     */
    @Override
    public void start() {
        for(SqlSourceCounter sqlSourceCounter : mapSqlSourceCounter.values()) {
            LOG.info("Starting sql source {} ...", sqlSourceCounter.getName());
            sqlSourceCounter.start();
        }
        super.start();
    }

    /**
     * Stop the source. Close database connection and stop metrics counter.
     */
    @Override
    public void stop() {
        try {
            hibernateHelper.closeSession();
            for(CSVWriter csvWriter : mapCSVWriter.values()) {
                csvWriter.close();
            }
        } catch (IOException e) {
            LOG.warn("Error CSVWriter object ", e);
        } finally {
            for(SqlSourceCounter sqlSourceCounter : mapSqlSourceCounter.values()) {
                LOG.info("Stopping sql source {} ...", sqlSourceCounter.getName());
                sqlSourceCounter.stop();
            }
            super.stop();
        }
    }

    private class ChannelWriter extends Writer {
        private List<Event> events = new ArrayList<>();
        private String tableName;

        public ChannelWriter(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            Event event = new SimpleEvent();

            String s = new String(cbuf);
            event.setBody(s.substring(off, len - 1).getBytes());

            Map<String, String> headers;
            headers = new HashMap<String, String>();
            headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
            headers.put("tablename", this.tableName);

            event.setHeaders(headers);

            events.add(event);

            if (events.size() >= batchSize)
                flush();
        }

        @Override
        public void flush() throws IOException {
            getChannelProcessor().processEventBatch(events);
            events.clear();
        }

        @Override
        public void close() throws IOException {
            flush();
        }
    }
}
