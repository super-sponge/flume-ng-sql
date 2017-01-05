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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Source to read data from a SQL database. This source ask for new data in a table each configured time.<p>
 * Created by sponge on 2016/12/26 0026.
 * @author <a href="mailto:super_sponge@163.com">liuhb</a>
 */
public class SQLSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger LOG = LoggerFactory.getLogger(SQLSource.class);
    protected SQLSourceHelper sqlSourceHelper;
    private SqlSourceCounter sqlSourceCounter;
    private CSVWriter csvWriter;
    private HibernateHelper hibernateHelper;

    /**
     * Configure the source, load configuration properties and establish connection with database
     */
    @Override
    public void configure(Context context) {

        LOG.getName();

        LOG.info("Reading and processing configuration values for source " + getName());

    	/* Initialize configuration parameters */
        sqlSourceHelper = new SQLSourceHelper(context, this.getName());

    	/* Initialize metric counters */
        sqlSourceCounter = new SqlSourceCounter("SOURCESQL." + this.getName());

        /* Establish connection with database */
        hibernateHelper = new HibernateHelper(sqlSourceHelper);
        hibernateHelper.establishSession();

        /* Instantiate the CSV Writer */
        csvWriter = new CSVWriter(new ChannelWriter());

    }

    /**
     * Process a batch of events performing SQL Queries
     */
    @Override
    public Status process() throws EventDeliveryException {

        try {
            sqlSourceCounter.startProcess();

            List<List<Object>> result = hibernateHelper.executeQuery();

            if (!result.isEmpty())
            {
                csvWriter.writeAll(sqlSourceHelper.getAllRows(result),true);
                csvWriter.flush();
                sqlSourceCounter.incrementEventCount(result.size());


                sqlSourceHelper.updateStatusFile();
            }

            sqlSourceCounter.endProcess(result.size());

            if (result.size() < sqlSourceHelper.getMaxRows()){
                Thread.sleep(sqlSourceHelper.getRunQueryDelay());
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

        LOG.info("Starting sql source {} ...", getName());
        sqlSourceCounter.start();
        super.start();
    }

    /**
     * Stop the source. Close database connection and stop metrics counter.
     */
    @Override
    public void stop() {

        LOG.info("Stopping sql source {} ...", getName());

        try
        {
            hibernateHelper.closeSession();
            csvWriter.close();
        } catch (IOException e) {
            LOG.warn("Error CSVWriter object ", e);
        } finally {
            this.sqlSourceCounter.stop();
            super.stop();
        }
    }

    private class ChannelWriter extends Writer {
        private List<Event> events = new ArrayList<>();

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            Event event = new SimpleEvent();

            String s = new String(cbuf);
            event.setBody(s.substring(off, len-1).getBytes());

            Map<String, String> headers;
            headers = new HashMap<String, String>();
            headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
            event.setHeaders(headers);

            events.add(event);

            if (events.size() >= sqlSourceHelper.getBatchSize())
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
