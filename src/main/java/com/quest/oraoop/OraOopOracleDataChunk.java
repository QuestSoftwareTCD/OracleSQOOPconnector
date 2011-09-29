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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;

public class OraOopOracleDataChunk {

    int id;
    int oracleDataObjectId;
    int relativeDatafileNumber;
    int startBlockNumber;
    int finishBlockNumber;

    OraOopOracleDataChunk() {

    }

    OraOopOracleDataChunk(int id, int oracleDataObjectId, int relativeDatafileNumber, int startBlockNumber, int finishBlockNumber) {

        this.id = id;
        this.oracleDataObjectId = oracleDataObjectId;
        this.relativeDatafileNumber = relativeDatafileNumber;
        this.startBlockNumber = startBlockNumber;
        this.finishBlockNumber = finishBlockNumber;
    }

    public void serialize(DataOutput output) throws IOException {

        output.writeInt(this.id);
        output.writeInt(this.oracleDataObjectId);
        output.writeInt(this.relativeDatafileNumber);
        output.writeInt(this.startBlockNumber);
        output.writeInt(this.finishBlockNumber);
    }

    public void deserialize(DataInput input) throws IOException {

        this.id = input.readInt();
        this.oracleDataObjectId = input.readInt();
        this.relativeDatafileNumber = input.readInt();
        this.startBlockNumber = input.readInt();
        this.finishBlockNumber = input.readInt();
    }

    public int getNumberOfBlocks() {

        if (this.finishBlockNumber == 0 && 
            this.startBlockNumber == 0)
            return 0;
        else
            return (this.finishBlockNumber - this.startBlockNumber) + 1;
    }

    @Override
    public String toString() {

        String result = super.toString();
        for (Field field : this.getClass().getDeclaredFields()) {
            try {
                Object fieldValue = field.get(this);
                result += String.format("\n\t%s = %s"
                                       ,field.getName()
                                       ,(fieldValue == null ? "null" : fieldValue.toString()));
            }
            catch (IllegalAccessException ex) {
            }
        }

        return result;
    }

}
