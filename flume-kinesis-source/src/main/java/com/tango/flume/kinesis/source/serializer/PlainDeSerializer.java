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
package com.tango.flume.kinesis.source.serializer;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.event.EventBuilder;
import com.amazonaws.services.kinesis.model.Record;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

public class PlainDeSerializer implements Serializer{
    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    @Override
    public void configure(Context context) {

    }

    @Override
    public void configure(ComponentConfiguration conf) {
    }

    @Override
    public Event parseEvent(Record record) throws KinesisSerializerException, CharacterCodingException {
        return EventBuilder.withBody(decoder.decode(record.getData()).toString().getBytes());
    }


}
