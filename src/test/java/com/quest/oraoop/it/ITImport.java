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
import java.util.ArrayList;
import java.util.List;
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
import com.quest.oraoop.test.OracleDataDefinition;

public class ITImport extends OraOopTestCase {
	
	private static OraOopLog LOG = OraOopLogFactory.getLog(ITImport.class.getName());
	
	@Test
	public void testProductImport() throws Exception {
		OracleConnection conn = getTestEnvConnection();
		int parallelProcesses = OracleData.getParallelProcesses(conn);
		int rowsPerSlave = Integer.valueOf(getTestEnvProperty("it_num_rows")) / parallelProcesses;
		
		List<OracleDataDefinition> columnList = new ArrayList<OracleDataDefinition>();
		
		columnList.add(new OracleDataDefinition("product_id","INTEGER","id"));
		columnList.add(new OracleDataDefinition("supplier_code","VARCHAR2 (30)","TO_CHAR (id - MOD (id, 5000),'FMXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')"));
		columnList.add(new OracleDataDefinition("product_code","VARCHAR2 (30)","TO_CHAR (MOD (id, 100000), 'FMXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')"));
		columnList.add(new OracleDataDefinition("product_descr","VARCHAR2 (255)","DBMS_RANDOM.string ('x', ROUND (DBMS_RANDOM.VALUE (1, 100)))"));
		columnList.add(new OracleDataDefinition("product_long_descr","VARCHAR2 (4000)","DBMS_RANDOM.string ('x', ROUND (DBMS_RANDOM.VALUE (1, 200)))"));
		columnList.add(new OracleDataDefinition("product_cost_price","NUMBER","ROUND (DBMS_RANDOM.VALUE (0, 100000), 2)"));
		columnList.add(new OracleDataDefinition("sell_from_date","DATE","TRUNC (SYSDATE + DBMS_RANDOM.VALUE (-365, 365))"));
		columnList.add(new OracleDataDefinition("sell_price","NUMBER","ROUND (DBMS_RANDOM.VALUE (0, 200000), 2)"));
		columnList.add(new OracleDataDefinition("create_user","VARCHAR2 (30)","DBMS_RANDOM.string ('U', 30)"));
		columnList.add(new OracleDataDefinition("create_time","TIMESTAMP","TO_TIMESTAMP (TO_CHAR (SYSDATE + DBMS_RANDOM.VALUE (-730, 0),'YYYYMMDDHH24MISS') || '.' || TRUNC (TO_CHAR (DBMS_RANDOM.VALUE * 999999999)), 'YYYYMMDDHH24MISSXFF')"));
		columnList.add(new OracleDataDefinition("last_update_user","VARCHAR2 (30)","DBMS_RANDOM.string ('U', 30)"));
		columnList.add(new OracleDataDefinition("last_update_time","TIMESTAMP","TO_TIMESTAMP (TO_CHAR (SYSDATE + DBMS_RANDOM.VALUE (-730, 0), 'YYYYMMDDHH24MISS') || '.' || TRUNC (TO_CHAR (DBMS_RANDOM.VALUE * 999999999)), 'YYYYMMDDHH24MISSXFF')"));
		
		try {
			long startTime = System.currentTimeMillis();
			OracleData.createTable(conn, "tst_product", columnList,parallelProcesses,rowsPerSlave);
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
				"com.quest.oraoop.loadtest.gen",
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
