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

import java.lang.reflect.Field;

import org.apache.hadoop.io.Writable;

public abstract class OraOopOracleDataChunk implements Writable {
	
    String id;
	
    public abstract int getNumberOfBlocks();
    
    public String getWhereClause() {
    	return "1=1";
    }
    
    public String getPartitionClause() {
    	return "";
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
