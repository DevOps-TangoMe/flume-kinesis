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


public class KinesisSinkConfigurationConstant {
	public static final String ACCESSKEY = "accessKey";
    public static final String ACCESSSECRETKEY ="accessSecretKey";
    public static final String KINESISPARTITIONS = "kinesisPartitions";
    public static final String STREAMNAME = "streamName";
    public static final String KINESISENDPOINT = "kinesisEndpoint";
    public static final String BATCH_SIZE = "batchSize";

    public static final Integer DEFAULT_BATCH_SIZE = 1;
}
