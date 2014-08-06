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


import org.apache.flume.channel.ChannelProcessor;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.tango.flume.kinesis.source.serializer.Serializer;

public class RecordProcessorFactory implements IRecordProcessorFactory{
    ChannelProcessor chProcessor;
    Serializer serializer = null;
    Long backOffTimeInMillis = null;
    Integer numberRetries = null;
    Long checkpointIntervalMillis = null;


    public RecordProcessorFactory(ChannelProcessor channelProcessor,
                                  Serializer serializer,
                                  Long backOffTimeInMillis,
                                  Integer numberRetries,
                                  Long checkpointIntervalMillis) {
        super();
        this.chProcessor = channelProcessor;
        this.serializer = serializer;
        this.backOffTimeInMillis = backOffTimeInMillis;
        this.numberRetries = numberRetries;
        this.checkpointIntervalMillis = checkpointIntervalMillis;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new FlumeSourceRecordProcessor(chProcessor,
                                              serializer,
                                              backOffTimeInMillis,
                                              numberRetries,
                                              checkpointIntervalMillis);
    }
}
