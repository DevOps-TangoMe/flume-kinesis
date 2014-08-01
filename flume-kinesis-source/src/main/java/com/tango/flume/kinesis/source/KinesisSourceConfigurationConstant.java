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

public class KinesisSourceConfigurationConstant implements AWSCredentialsProvider {

    //testing start
    public static final String ACCESSKEY = "accessKey";
    public static final String ACCESSSECRETKEY ="accessSecretKey";
    public static final String APPLICATIONNAME = "kinesisApplicationName";
    public static final String STREAMNAME = "kinesisStreamName";
    public static final String KINESISENDPOINT = "kinesisEndpoint";
    public static final String INITIALPOSITION = "initialPosition";

    //testing end



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
