/**
 *  Copyright 2014 TangoMe Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.tango.flume.kinesis.source;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.SimpleEvent;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlumeSourceRecordProcessor implements IRecordProcessor{
    private static final Logger logger = LoggerFactory.getLogger(FlumeSourceRecordProcessor.class);
    private String kinesisShardId;

    private static final String TIMESTAMP = "timestamp";

    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;

    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;

    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();


    ChannelProcessor chProcessor;
    public FlumeSourceRecordProcessor(ChannelProcessor chProcessor) {
        super();
        this.chProcessor = chProcessor;
    }

    @Override
    public void initialize(String shardId) {
        logger.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        if (logger.isDebugEnabled()) {
            logger.debug("Processing " + records.size() + " records from " + kinesisShardId);
        }

        // Process records and perform all exception handling.
        processRecordsWithRetries(records);

        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    /** Process records performing retries as needed. Skip "poison pill" records.
     * @param records
     */
    private void processRecordsWithRetries(List<Record> records) {

        Event event;
        Map<String, String> headers;

        ArrayList<Event> eventList=new ArrayList<Event>(records.size());

        for (Record record : records) {
            boolean processedSuccessfully = false;

            for (int i = 0; i < NUM_RETRIES; i++) {
                try {

                    event = new SimpleEvent();
                    headers = new HashMap<String, String>();
                    headers.put(FlumeSourceRecordProcessor.TIMESTAMP, String.valueOf(System.currentTimeMillis()));
                    String data = decoder.decode(record.getData()).toString();
                    if (logger.isDebugEnabled()) {
                        logger.debug(record.getSequenceNumber() + ", " + record.getPartitionKey()+":"+data);
                    }
                    event.setBody(data.getBytes());
                    event.setHeaders(headers);
                    eventList.add(event);

                    processedSuccessfully = true;
                    break;

                } catch (Throwable t) {
                    logger.error("Caught throwable while processing record " + record, t);
                }

                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    logger.error("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                logger.error("Couldn't process record " + record + ". Skipping the record.");
            }

        }
        if (logger.isDebugEnabled()) {
            logger.debug("event size after: " + eventList.size());
        }


        int retryTimes = 0;
        while (true) {
            try {
                chProcessor.processEventBatch(eventList);
                break;
            } catch (ChannelException ce) {
                logger.error("Error processEventBatch. Sleep and retry {} time(s)", ++retryTimes, ce);

                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException ie) {
                    logger.error("Interrupted sleep", ie);
                }


            } catch (Throwable t) {
                logger.error("Error processEventBatch. Sleep and retry {} time(s)", ++retryTimes, t);

                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException ie) {
                    logger.error("Interrupted sleep", ie);
                }
            }
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        logger.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }


    /** Checkpoint with retries.
     * @param checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        if (logger.isDebugEnabled()) {
            logger.debug("Checkpointing shard " + kinesisShardId);
        }
        boolean done = false;
        for (int i = 0; i < NUM_RETRIES && !done; i++) {
            try {
                checkpointer.checkpoint();
                done = true;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                logger.error("Caught shutdown exception, skipping checkpoint.", se);
                done = true;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                logger.error("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                        + NUM_RETRIES, e);
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                logger.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                done = true;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                logger.error("Interrupted sleep", e);
            }
        }
    }

}
