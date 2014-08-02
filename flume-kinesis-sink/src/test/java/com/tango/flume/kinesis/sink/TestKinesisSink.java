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

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import org.apache.flume.*;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;


public class TestKinesisSink {

    @Test
    public void simpleProcessTest() throws EventDeliveryException {
        Channel channel = mock(Channel.class);
        Transaction transactionMock = mock(Transaction.class);
        AmazonKinesisClient kinesisClient = mock(AmazonKinesisClient.class);
        PutRecordResult putRecordResult = mock(PutRecordResult.class);


        when(channel.getTransaction()).thenReturn(transactionMock);

        Event testEvent = new SimpleEvent();
        byte[] testBody = new byte[]{'b', 'o', 'd', 'y'};
        testEvent.setBody(testBody);
        when(channel.take()).thenReturn(testEvent);


        when(kinesisClient.putRecord(any(PutRecordRequest.class))).thenReturn(putRecordResult);

        KinesisSink kinesisSink = new KinesisSink(kinesisClient);
        kinesisSink.setChannel(channel);

        Context context = new Context();
        context.put(KinesisSinkConfigurationConstant.ACCESS_KEY, "default");
        context.put(KinesisSinkConfigurationConstant.ACCESS_SECRET_KEY, "default");
        context.put(KinesisSinkConfigurationConstant.STREAM_NAME, "default");
        kinesisSink.configure(context);

        kinesisSink.start();

        kinesisSink.process();

        verify(channel, times(1)).getTransaction();
        verify(channel, times(1)).take();

        verify(transactionMock, times(1)).begin();
        verify(transactionMock, times(1)).close();
        verify(transactionMock, times(1)).commit();
        verify(transactionMock, times(0)).rollback();
    }

}
