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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.Sqoop;
import com.quest.oraoop.test.HadoopFiles;
import com.quest.oraoop.test.OracleData;

import oracle.jdbc.OracleConnection;

public abstract class OraOopTestCase {
	
	protected static OraOopLog LOG = OraOopLogFactory.getLog(OraOopTestCase.class.getName());
	
	private String sqoopGenLibDirectory = System.getProperty("user.dir") + "/target/tmp/lib";
	private String sqoopGenSrcDirectory = System.getProperty("user.dir") + "/target/tmp/src";
	private String sqoopTargetDirectory = "target/tmp/";
	
	protected OracleConnection conn;
	
	protected ClassLoader classLoader;
	{
	  classLoader = Thread.currentThread().getContextClassLoader();
	  if (classLoader == null) {
	    classLoader = OraOopTestCase.class.getClassLoader();
	  }
	}
	
    static {
        Configuration.addDefaultResource(OraOopConstants.ORAOOP_SITE_TEMPLATE_FILENAME);
        Configuration.addDefaultResource(OraOopConstants.ORAOOP_SITE_FILENAME);
    }
	
	private final String configurationFileName = "oraoop-test-env.properties";
	private final String configurationDefaultFileName = "oraoop-test-env.properties.default";
	
	private Properties conf = new Properties();
	
	public OraOopTestCase() {
		URL url = null;
		url = classLoader.getResource(configurationFileName);
		if (url == null) {
			url = classLoader.getResource(configurationDefaultFileName);
			if (url == null) {
				throw new RuntimeException("Could not find " + configurationFileName);
			}
		}
		try {
			conf.load(url.openStream());
		} catch (IOException e) {
			throw new RuntimeException("Error loading " + configurationFileName,e);
		}
		try {
			classLoader.loadClass("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error loading Oracle JDBC driver",e);
		}
	}
	
	protected String getSqoopTargetDirectory() {
		        return sqoopTargetDirectory;
	}

	protected void setSqoopTargetDirectory(String sqoopTargetDirectory) {
		this.sqoopTargetDirectory = sqoopTargetDirectory;
	}

	protected String getSqoopGenLibDirectory() {
	    return sqoopGenLibDirectory;
	}

	protected String getSqoopGenSrcDirectory() {
	   return sqoopGenSrcDirectory;
	}       

	protected String getTestEnvProperty(String name) {
		return this.conf.getProperty(name);
	}
	
	protected void setTestEnvProperty(String name, String value) {
		this.conf.setProperty(name, value);
	}

	
	protected OracleConnection getTestEnvConnection() throws SQLException {
		if (this.conn == null) {
			this.conn = (OracleConnection) DriverManager.getConnection(this.getTestEnvProperty("oracle_url"),this.getTestEnvProperty("oracle_username"),this.getTestEnvProperty("oracle_password"));
		}
		return this.conn;
	}
	
	protected void closeTestEnvConnection() {
		try {
			if (this.conn != null) {
				this.conn.close();
			}
		} catch(SQLException e) {
			//Tried to close connection but failed - continue anyway
		}
		this.conn = null;
	}
	
	protected void createTable(String fileName) {
		try {
			OracleConnection conn = getTestEnvConnection();
			int parallelProcesses = OracleData.getParallelProcesses(conn);
			int rowsPerSlave = Integer.valueOf(getTestEnvProperty("it_num_rows")) / parallelProcesses;
			try {
				long startTime = System.currentTimeMillis();
				OracleData.createTable(conn, fileName, parallelProcesses, rowsPerSlave);
				LOG.info("Created and loaded table in " + ((System.currentTimeMillis() - startTime)/1000) + " seconds.");
			} catch (SQLException e) {
				if(e.getErrorCode()==955) {
					LOG.info("Table already exists - using existing data");
				} else {
					throw new RuntimeException(e);
				}
			}
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	protected Configuration getSqoopConf() {
		Configuration sqoopConf = new Configuration();
		sqoopConf.set("sqoop.connection.factories","com.quest.oraoop.OraOopManagerFactory");
		if (getTestEnvProperty("dfs_replication") != null) {
			sqoopConf.set("dfs.replication", getTestEnvProperty("dfs_replication"));
		}
		return sqoopConf;
	}
	
	protected int runImport(String tableName) {
		return runImport(tableName, false);
	}
	
	protected int runImport(String tableName, boolean sequenceFile) {
		return runImport(tableName,sequenceFile, OraOopConstants.ORAOOP_ORACLE_DATA_CHUNK_METHOD_DEFAULT, null);
	}

	protected int runImport(String tableName, boolean sequenceFile, OraOopConstants.OraOopOracleDataChunkMethod dataChunkMethod, String partitionList) {
		List<String> sqoopArgs = new ArrayList<String>();

		sqoopArgs.add("import");

		if (sequenceFile) {
			sqoopArgs.add("--as-sequencefile");
		}

		sqoopArgs.add("--connect");
		sqoopArgs.add(getTestEnvProperty("oracle_url"));

		sqoopArgs.add("--username");
		sqoopArgs.add(getTestEnvProperty("oracle_username"));

		sqoopArgs.add("--password");
		sqoopArgs.add(getTestEnvProperty("oracle_password"));

		sqoopArgs.add("--table");
		sqoopArgs.add(tableName);

		sqoopArgs.add("--target-dir");
		sqoopArgs.add(this.sqoopTargetDirectory);

		sqoopArgs.add("--package-name");
		sqoopArgs.add("com.quest.oraoop.gen");

		sqoopArgs.add("--bindir");
		sqoopArgs.add(this.sqoopGenLibDirectory);

		sqoopArgs.add("--outdir");
		sqoopArgs.add(this.sqoopGenSrcDirectory);

		if (getTestEnvProperty("num_mappers") != null) {
			sqoopArgs.add("--num-mappers");
			sqoopArgs.add(getTestEnvProperty("num_mappers"));
		}

		Configuration sqoopConf = getSqoopConf();
		sqoopConf.set(OraOopConstants.ORAOOP_ORACLE_DATA_CHUNK_METHOD, dataChunkMethod.toString());
		
		if(partitionList!=null && !partitionList.isEmpty()) {
		  sqoopConf.set(OraOopConstants.ORAOOP_IMPORT_PARTITION_LIST, partitionList);
		}
		
		return Sqoop.runTool(sqoopArgs.toArray(new String[sqoopArgs.size()]),sqoopConf);
	}
	
	protected int runExportFromTemplateTable(String templateTableName, String tableName) {
		List<String> sqoopArgs = new ArrayList<String>();

		sqoopArgs.add("export");

		sqoopArgs.add("--connect");
		sqoopArgs.add(getTestEnvProperty("oracle_url"));

		sqoopArgs.add("--username");
		sqoopArgs.add(getTestEnvProperty("oracle_username"));

		sqoopArgs.add("--password");
		sqoopArgs.add(getTestEnvProperty("oracle_password"));

		sqoopArgs.add("--table");
		sqoopArgs.add(tableName);

		sqoopArgs.add("--export-dir");
		sqoopArgs.add(this.sqoopTargetDirectory);

		sqoopArgs.add("--package-name");
		sqoopArgs.add("com.quest.oraoop.gen");

		sqoopArgs.add("--bindir");
		sqoopArgs.add(this.sqoopGenLibDirectory);

		sqoopArgs.add("--outdir");
		sqoopArgs.add(this.sqoopGenSrcDirectory);

		Configuration sqoopConf = getSqoopConf();

		sqoopConf.set("oraoop.template.table",templateTableName);
		sqoopConf.setBoolean("oraoop.drop.table",true);
		sqoopConf.setBoolean("oraoop.nologging",true);
		sqoopConf.setBoolean("oraoop.partitioned",false);

		return Sqoop.runTool(sqoopArgs.toArray(new String[sqoopArgs.size()]),sqoopConf);
	}
	
	protected int runCompareTables(Connection conn, String table1, String table2) throws SQLException {
		PreparedStatement stmt;
		stmt = conn.prepareStatement("select count(*) from (select * from (select * from " +
		                                                                       table1 +
		                                                                       " minus select * from " +
		                                                                       table2 +
		                                                                       ") union all select * from (select * from " +
		                                                                       table2+ " minus select * from "+
		                                                                       table1+"))");
		ResultSet results = stmt.executeQuery();
		results.next();
		int numDifferences = results.getInt(1);
		return numDifferences;
	}

	protected void cleanupFolders() throws Exception {
		HadoopFiles.deleteFolder(getSqoopTargetDirectory());
		HadoopFiles.deleteFolder(new File(getSqoopGenSrcDirectory()).toURI().toURL().toString());
		HadoopFiles.deleteFolder(new File(getSqoopGenLibDirectory()).toURI().toURL().toString());
	}

}
