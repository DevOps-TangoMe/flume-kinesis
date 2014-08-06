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

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;

import com.tango.flume.kinesis.source.serializer.KinesisSerializerException;
import com.tango.flume.kinesis.source.serializer.Serializer;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;

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
    private String kinesisShardId = null;

    // Backoff and retry settings
    private Long backoffTimeInMillis = null;
    private Integer numberRetries = null;
    private Serializer serializer = null;

    // Checkpoint about once a minute
    private static Long checkpointIntervalMillis = null;
    private long nextCheckpointTimeInMillis;

    ChannelProcessor chProcessor;
    public FlumeSourceRecordProcessor(ChannelProcessor chProcessor,
                                      Serializer serializer,
                                      Long backOffTimeInMillis,
                                      Integer numberRetries,
                                      Long checkpointIntervalMillis) {
        super();
        this.chProcessor = chProcessor;
        this.numberRetries = numberRetries;
        this.backoffTimeInMillis = backOffTimeInMillis;
        this.checkpointIntervalMillis = checkpointIntervalMillis;
        this.serializer = serializer;

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
            nextCheckpointTimeInMillis = System.currentTimeMillis() + checkpointIntervalMillis;
        }
    }

    /** Process records performing retries as needed. Skip "poison pill" records.
     * @param records
     */
    private void processRecordsWithRetries(List<Record> records) {


        List<Event> flumeEvents = new ArrayList<Event>(records.size());
        for (Record record : records) {
            boolean processedSuccessfully = false;

            for (int i = 0; i < numberRetries; i++) {
                try{
                    Event flumeEvent = serializer.parseEvent(record);
                    if(flumeEvent != null && logger.isTraceEnabled()){
                        logger.trace("Event parsed as :" + flumeEvent.toString());
                    }
                    flumeEvents.add(flumeEvent);
                    processedSuccessfully = true;
                    break;

                }catch (CharacterCodingException  e){
                    logger.error("Error while decoding record " + record, e);

                }catch (KinesisSerializerException e){
                    logger.error("Error while serializing record " + record, e);

                }catch (Throwable t) {
                    logger.error("Caught throwable while processing record " + record, t);
                }

                try {
                    Thread.sleep(backoffTimeInMillis);
                } catch (InterruptedException e) {
                    logger.error("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                logger.error("Couldn't process record: " + record + ". Skipping the record.");
            }

        }



        if (logger.isDebugEnabled()) {
            logger.debug("event size after: " + flumeEvents.size());
        }


        int retryTimes = 0;
        while (true) {
            try {
                chProcessor.processEventBatch(flumeEvents);
                break;
            } catch (ChannelException ce) {
                logger.error("Error processEventBatch. Sleep and retry {} time(s)", ++retryTimes, ce);

                try {
                    Thread.sleep(backoffTimeInMillis);
                } catch (InterruptedException ie) {
                    logger.error("Interrupted sleep", ie);
                }


            } catch (Throwable t) {
                logger.error("Error processEventBatch. Sleep and retry {} time(s)", ++retryTimes, t);

                try {
                    Thread.sleep(backoffTimeInMillis);
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
        for (int i = 0; i < numberRetries && !done; i++) {
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
                        + numberRetries, e);
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                logger.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                done = true;
            }
            try {
                Thread.sleep(backoffTimeInMillis);
            } catch (InterruptedException e) {
                logger.error("Interrupted sleep", e);
            }
        }
    }

}
