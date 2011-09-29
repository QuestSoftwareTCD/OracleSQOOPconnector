/**
 *   Copyright 2011 Quest Software, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.quest.oraoop;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

public class OraOopSerializationFactory<T> implements Serialization<T> {

    private static final OraOopLog LOG = OraOopLogFactory.getLog(OraOopSerializationFactory.class);

    @Override
    public boolean accept(Class<?> c) {

        boolean result = false;

        if (c == com.quest.oraoop.OraOopDBInputSplit.class)
            result = true;

        LOG.debug(String.format("accept(%s) returned %s"
                               ,c.getName()
                               ,result));

        return result;
    }

    @Override
    public Deserializer<T> getDeserializer(Class<T> c) {

        return new OraOopDBInputSplitSerializer<T>();
    }

    @Override
    public Serializer<T> getSerializer(Class<T> c) {

        return new OraOopDBInputSplitSerializer<T>();
    }

}
