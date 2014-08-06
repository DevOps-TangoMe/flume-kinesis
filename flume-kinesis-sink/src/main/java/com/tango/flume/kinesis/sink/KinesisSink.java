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
package com.tango.flume.kinesis.sink;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.tango.flume.kinesis.sink.serializer.Serializer;
import com.tango.flume.kinesis.sink.serializer.KinesisSerializerException;

public class KinesisSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(KinesisSink.class);
    private AmazonKinesisClient kinesisClient;

    /**
     * Configuration attributes
     */

    private String accessKey = null;
    private String accessSecretKey = null;
    private String streamName = null;
    private String numberOfPartitions = null;
    private String kinesisEndpoint = null;
    private Integer batchSize = null;
    private Serializer serializer = null;
    private static Long backOffTimeInMillis = null;
    private static Integer numberRetries = null;



    public KinesisSink() {

    }

    @VisibleForTesting
    public KinesisSink(AmazonKinesisClient kinesisClient) {
        this.kinesisClient = kinesisClient;
        this.kinesisClient.setEndpoint(kinesisEndpoint);
    }

    @Override
    public synchronized void start() {
        logger.info("Starting KinesisSink:  " + this.getName());
        if (this.kinesisClient == null) {
            this.kinesisClient = new AmazonKinesisClient(new BasicAWSCredentials(this.accessKey, this.accessSecretKey));
        }
        this.kinesisClient.setEndpoint(kinesisEndpoint);
        super.start();
    }

    @Override
    public synchronized void stop() {
        logger.info("Stoping KinesisSink: " + this.getName());
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;

        // Start transaction

        List<byte[]> batchEvents = new ArrayList<byte[]>(batchSize);
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();

        try {
            txn.begin();

            for (int i = 0; i < batchSize && status != Status.BACKOFF; i++) {
                Event event = ch.take();
                if (event == null) {
                    status = Status.BACKOFF;
                } else {
                    try{
                        batchEvents.add(serializer.serialize(event));
                    }catch (KinesisSerializerException e){
                        logger.error("Could not serialize event: " + event, e);
                    }

                }
            }

            /**
             * Only send events if we got any
             */
            if (batchEvents.size() > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Sending " + batchEvents.size() + " events");
                }

                for (byte[] event: batchEvents) {
                    int partitionKey = new Random().nextInt((Integer.valueOf(numberOfPartitions) - 1) + 1) + 1;
                    PutRecordRequest putRecordRequest = new PutRecordRequest();
                    putRecordRequest.setStreamName(this.streamName);
                    putRecordRequest.setData(ByteBuffer.wrap(event));
                    putRecordRequest.setPartitionKey("partitionKey_" + partitionKey);

                    int attempt = 0;
                    while (true) {
                        try {
                            PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);
                            if (logger.isDebugEnabled()) {
                                logger.debug("Record put to kinesis --- {}", event.toString());
                            }

                            break;
                        } catch (ProvisionedThroughputExceededException e) {
                            if (++attempt == numberRetries) {
                                Throwables.propagate(e);
                            }
                            try {
                                Thread.sleep(backOffTimeInMillis);
                            } catch (InterruptedException ie) {
                                logger.error("Interrupted sleep", ie);
                            }
                        }
                    }
                }
            }

            txn.commit();
        } catch (AmazonClientException e) {
            txn.rollback();
            logger.error("Can't put record to amazon kinesis", e);
            status = Status.BACKOFF;
        } catch (Throwable t) {
            txn.rollback();
            logger.error("Transaction failed ", t);
            // Log exception, handle individual exceptions as needed
            status = Status.BACKOFF;
            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        } finally {
            txn.close();
        }


        return status;
    }

    @Override
    public void configure(Context context) {
        this.accessKey = context.getString(KinesisSinkConfigurationConstant.ACCESS_KEY);
        if (StringUtils.isBlank(this.accessKey)) {
            logger.error("Access key cannot be blank");
            throw new IllegalArgumentException("Access key cannot be blank");
        }

        this.accessSecretKey = context.getString(KinesisSinkConfigurationConstant.ACCESS_SECRET_KEY);
        if (StringUtils.isBlank(this.accessSecretKey)) {
            logger.error("Access secret key cannot be blank");
            throw new IllegalArgumentException("Access secret key cannot be blank");
        }

        this.numberOfPartitions = context.getString(KinesisSinkConfigurationConstant.KINESIS_PARTITIONS,
                                                    KinesisSinkConfigurationConstant.DEFAULT_KINESIS_PARTITIONS);

        this.streamName = context.getString(KinesisSinkConfigurationConstant.STREAM_NAME);
        if(StringUtils.isBlank(this.streamName)){
            logger.error("Stream name cannot be blank");
            throw new IllegalArgumentException("Stream name cannot be blank");
        }

        this.kinesisEndpoint = context.getString(KinesisSinkConfigurationConstant.KINESIS_ENDPOINT,
                                                 KinesisSinkConfigurationConstant.DEFAULT_KINESIS_ENDPOINT);

        this.batchSize = context.getInteger(KinesisSinkConfigurationConstant.BATCH_SIZE,
                                            KinesisSinkConfigurationConstant.DEFAULT_BATCH_SIZE);

        Preconditions.checkState(batchSize > 0, KinesisSinkConfigurationConstant.BATCH_SIZE
                + " parameter must be greater than 1");

        this.backOffTimeInMillis = context.getLong(KinesisSinkConfigurationConstant.BACKOFF_TIME_IN_MILLIS,
                                                   KinesisSinkConfigurationConstant.DEFAUTL_BACKOFF_TIME_IN_MILLIS);

        this.numberRetries = context.getInteger(KinesisSinkConfigurationConstant.NUM_RETRIES,
                                                KinesisSinkConfigurationConstant.DEFAULT_NUM_RETRIES);

        //serializer
        String serializerClassName = context.getString(KinesisSinkConfigurationConstant.SERIALIZER,
                                                       KinesisSinkConfigurationConstant.DEFAULT_SERIALIZER_CLASS_NAME);
        try {
            /**
             * Instantiate serializer
             */
            @SuppressWarnings("unchecked") Class<? extends Serializer> clazz = (Class<? extends Serializer>) Class
                    .forName(serializerClassName);
            serializer = clazz.newInstance();

            /**
             * Configure it
             */
            Context serializerContext = new Context();
            serializerContext.putAll(context.getSubProperties(KinesisSinkConfigurationConstant.SERIALIZER_PREFIX));
            serializer.configure(serializerContext);

        } catch (ClassNotFoundException e) {
            logger.error("Could not instantiate event serializer", e);
            Throwables.propagate(e);
        } catch (InstantiationException e) {
            logger.error("Could not instantiate event serializer", e);
            Throwables.propagate(e);
        } catch (IllegalAccessException e) {
            logger.error("Could not instantiate event serializer", e);
            Throwables.propagate(e);
        }

    }
}
