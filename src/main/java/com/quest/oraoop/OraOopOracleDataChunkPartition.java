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

import org.apache.hadoop.io.Text;

public class OraOopOracleDataChunkPartition extends OraOopOracleDataChunk {
	
	boolean isSubPartition;
	int blocks;
	
	OraOopOracleDataChunkPartition() {
		
	}
	
	OraOopOracleDataChunkPartition(String partitionName, boolean isSubPartition, int blocks) {
		this.id = partitionName;
		this.isSubPartition = isSubPartition;
		this.blocks = blocks;
	}

	@Override
	public int getNumberOfBlocks() {
		return this.blocks;
	}

	@Override
	public void write(DataOutput output) throws IOException {
		Text.writeString(output,this.id);
		output.writeBoolean(this.isSubPartition);
		output.writeInt(this.blocks);
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		 this.id = Text.readString(input);
	     this.isSubPartition = input.readBoolean();
	     this.blocks = input.readInt();
	}

	@Override
	public String getPartitionClause() {
		StringBuilder sb = new StringBuilder();
		sb.append(" ");
		if (this.isSubPartition) {
			sb.append("SUBPARTITION");
		} else {
			sb.append("PARTITION");
		}
		sb.append("(\"").append(this.id).append("\")");
		return sb.toString();
	}

}
