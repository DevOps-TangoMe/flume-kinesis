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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.tango.flume.kinesis.source.serializer.PlainDeSerializer;

public class KinesisSourceConfigurationConstant implements AWSCredentialsProvider {


    public static final String ACCESS_KEY = "accessKey";
    public static final String ACCESS_SECRET_KEY ="accessSecretKey";
    public static final String APPLICATION_NAME = "kinesisApplicationName";
    public static final String STREAM_NAME = "kinesisStreamName";
    public static final String KINESIS_ENDPOINT = "kinesisEndpoint";
    public static final String INITIAL_POSITION = "initialPosition";

    public static final String SERIALIZER = "serializer";
    public static final String SERIALIZER_PREFIX = "serializer.";

    public static final String BACKOFF_TIME_IN_MILLIS = "backOffTimeInMillis";
    public static final String NUM_RETRIES = "numberRetries";
    public static final String CHECKPOINT_INTERVAL_MILLIS = "checkpointIntervalMillis";
    public static final String FAILOVER_TIME_MILLIS = "failoverTimeMillis";

    public static final Long DEFAUTL_BACKOFF_TIME_IN_MILLIS = 3000L;
    public static final Integer DEFAULT_NUM_RETRIES = 10;
    public static final Long DEFAULT_CHECKPOINT_INTERVAL_MILLIS = 60000L;
    public static final Long DEFAULT_FAILOVER_TIME_MILLIS = 60000L;
    public static final String DEFAULT_KINESIS_ENDPOINT = "https://kinesis.us-west-2.amazonaws.com";
    public static final String DEFAULT_SERIALIZER_CLASS_NAME = PlainDeSerializer.class.getName();


    String accessKey;
    String accessSecretKey;

    public KinesisSourceConfigurationConstant(String accessKey,String accessSecretKey){
        this.accessKey = accessKey;
        this.accessSecretKey = accessSecretKey;
    }

    @Override
    public AWSCredentials getCredentials(){
        return new BasicAWSCredentials(this.accessKey,this.accessSecretKey);
    }

    @Override
    public void refresh() {

    }

}
