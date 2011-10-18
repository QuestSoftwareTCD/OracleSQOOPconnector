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

package com.quest.oraoop.it;


import java.io.File;
import java.sql.SQLException;
import junit.framework.Assert;
import oracle.jdbc.OracleConnection;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import com.cloudera.sqoop.Sqoop;
import com.quest.oraoop.OraOopLog;
import com.quest.oraoop.OraOopLogFactory;
import com.quest.oraoop.OraOopTestCase;
import com.quest.oraoop.test.HadoopFiles;
import com.quest.oraoop.test.OracleData;

public class ITImport extends OraOopTestCase {
	
	private static OraOopLog LOG = OraOopLogFactory.getLog(ITImport.class.getName());
	
	@Test
	public void testProductImport() throws Exception {
		OracleConnection conn = getTestEnvConnection();
		int parallelProcesses = OracleData.getParallelProcesses(conn);
		int rowsPerSlave = Integer.valueOf(getTestEnvProperty("it_num_rows")) / parallelProcesses;

		try {
			long startTime = System.currentTimeMillis();
			OracleData.createTable(conn, "table_tst_product.xml", parallelProcesses, rowsPerSlave);
			LOG.info("Created and loaded table in " + ((System.currentTimeMillis() - startTime)/1000) + " seconds.");
		} catch (SQLException e) {
			if(e.getErrorCode()==955) {
				LOG.info("Table already exists - using existing data");
			} else {
				throw e;
			}
		}
		
		try {
		
			String[] sqoopArgs = {"import",
				"--connect",
				getTestEnvProperty("oracle_url"),
				"--username",
				getTestEnvProperty("oracle_username"),
				"--password",
				getTestEnvProperty("oracle_password"),
				"--table",
				"tst_product",
				"--target-dir",
				this.sqoopTargetDirectory + "tst_product",
				"--package-name",
				"com.quest.oraoop.it.gen",
				"--bindir",
				this.sqoopGenLibDirectory,
				"--outdir",
				this.sqoopGenSrcDirectory
				};
			Configuration sqoopConf = getSqoopConf();
			int retCode = Sqoop.runTool(sqoopArgs,sqoopConf);
			Assert.assertEquals("Return code should be 0", 0,retCode);
			
		} finally {
			HadoopFiles.deleteFolder(this.sqoopTargetDirectory + "tst_product");
			HadoopFiles.deleteFolder(new File(this.sqoopGenSrcDirectory).toURI().toURL().toString());
			HadoopFiles.deleteFolder(new File(this.sqoopGenLibDirectory).toURI().toURL().toString());
		}
	}

}
