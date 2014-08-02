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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;


import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisSource extends AbstractSource implements Configurable, EventDrivenSource {
    private static final Logger logger = LoggerFactory.getLogger(KinesisSource.class);
    private Worker worker;

    // Initial position in the stream when the application starts up for the first time.
    // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
    private InitialPositionInStream DEFAULT_INITIAL_POSITION = InitialPositionInStream.TRIM_HORIZON;
    private static final String DEFAULT_KINESIS_ENDPOINT = "https://kinesis.us-west-2.amazonaws.com";
    private String applicationName;
    private String streamName;
    private String kinesisEndpoint = DEFAULT_KINESIS_ENDPOINT;
    private String initialPosition;

    private KinesisClientLibConfiguration kinesisClientLibConfiguration;
    private String accessKey;
    private String accessSecretKey;



    public KinesisSource(){

    }


    @Override
    public void configure(Context context) {

        String workerId=null;
        AWSCredentialsProvider credentialsProvider = null;

        this.accessKey = context.getString(KinesisSourceConfigurationConstant.ACCESS_KEY);
        if (StringUtils.isBlank(this.accessKey)) {
            throw new IllegalArgumentException("ACCESS_KEY cannot be blank");
        }

        this.accessSecretKey = context.getString(KinesisSourceConfigurationConstant.ACCESS_SECRET_KEY);
        if (StringUtils.isBlank(this.accessSecretKey)) {
            throw new IllegalArgumentException("ACCESS_SECRET_KEY cannot be blank");
        }

        this.applicationName = context.getString(KinesisSourceConfigurationConstant.APPLICATION_NAME);
        if(StringUtils.isBlank(this.applicationName)){
            logger.error("Application name cannot be blank");
            throw new IllegalArgumentException("Application name cannot be blank");
        }

        this.streamName = context.getString(KinesisSourceConfigurationConstant.STREAM_NAME);
        if(StringUtils.isBlank(this.streamName)){
            logger.error("Stream name cannot be blank");
            throw new IllegalArgumentException("Stream name cannot be blank");
        }

        this.kinesisEndpoint = context.getString(KinesisSourceConfigurationConstant.KINESIS_ENDPOINT, DEFAULT_KINESIS_ENDPOINT);
        this.initialPosition = context.getString(KinesisSourceConfigurationConstant.INITIAL_POSITION, "TRIM_HORIZON");


        if (initialPosition.equals("LATEST")){
            DEFAULT_INITIAL_POSITION=InitialPositionInStream.LATEST;
        }


        try{
            credentialsProvider = new KinesisSourceConfigurationConstant(accessKey,accessSecretKey);
            logger.info("Obtained credentials from the properties file.");

        } catch(AmazonClientException e){
            logger.error("Credentials are not matched", e);
            com.google.common.base.Throwables.propagate(e);
        }

        try {
            workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            logger.error("Fail to generate workerID", e);
            com.google.common.base.Throwables.propagate(e);

        }


        logger.info("Using workerId: " + workerId);

        kinesisClientLibConfiguration = new KinesisClientLibConfiguration(applicationName, streamName,
                credentialsProvider, workerId).
                withKinesisEndpoint(kinesisEndpoint).
                withInitialPositionInStream(DEFAULT_INITIAL_POSITION);

    }

    @Override
    public void start() {

        IRecordProcessorFactory recordProcessorFactory = new RecordProcessorFactory(getChannelProcessor());
        worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting down Kinesis client thread...");
                worker.shutdown();
            }
        });

        try{
            worker.run();
        }catch (AmazonClientException e) {
            logger.error("Can't connect to amazon kinesis", e);
            com.google.common.base.Throwables.propagate(e);
        }

    }

    @Override
    public void stop() {

    }



}
