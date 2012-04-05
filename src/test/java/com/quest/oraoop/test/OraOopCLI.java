/**
 *   Copyright 2012 Quest Software, Inc.
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

package com.quest.oraoop.test;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import com.quest.oraoop.OraOopLog;
import com.quest.oraoop.OraOopLogFactory;
import com.quest.oraoop.OraOopTestCase;

public class OraOopCLI extends OraOopTestCase {
	
	private static OraOopLog LOG = OraOopLogFactory.getLog(OraOopCLI.class.getName());
	
	private static final Options options = new Options();
	static
	{
		
		OptionBuilder.withLongOpt("connect");
		OptionBuilder.withArgName("jdbc-uri");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("Oracle JDBC connect string");
		options.addOption(OptionBuilder.create("c"));
		
		OptionBuilder.withLongOpt("username");
		OptionBuilder.withArgName("username");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("Oracle username");
		options.addOption(OptionBuilder.create("u"));
		
		OptionBuilder.withLongOpt("password");
		OptionBuilder.withArgName("password");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("Oracle password");
		options.addOption(OptionBuilder.create("p"));
		
		OptionBuilder.withLongOpt("parallel-degree");
		OptionBuilder.withArgName("int");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("Degree of parallelism to use to load Oracle table");
		options.addOption(OptionBuilder.create("d"));
		
		OptionBuilder.withLongOpt("num-rows");
		OptionBuilder.withArgName("int");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("Number of rows to load into Oracle table");
		options.addOption(OptionBuilder.create("r"));
		
		OptionBuilder.withLongOpt("num-mappers");
		OptionBuilder.withArgName("int");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("Number of mappers to use");
		options.addOption(OptionBuilder.create("m"));
		
		OptionBuilder.withLongOpt("dfs-replication");
		OptionBuilder.withArgName("int");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("DFS replication");
		options.addOption(OptionBuilder.create());

	}
	
	public static void printHelp() {
		String usage = OraOopCLI.class.getName() + " [options] action-name [args...]";
		String header = "\nList of Options:";
		String footer = "\nList of actions:\n" +
						"create-table <file name>\n" +
						"import-table <table name> <target directory>\n" +
						"export-table-from-template <template table> <target table> <target directory>\n" +
						"compare-tables <table1> <table2>\n" +
						"round-trip <file name>";
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(100);
		formatter.printHelp(usage, header, options, footer);
	}
	
	public void createTable(Connection conn, String fileName) throws Exception {
		String parallelDegreeProperty = getTestEnvProperty("parallel_degree");
		int parallelDegree = 0;
		if (parallelDegreeProperty != null && !parallelDegreeProperty.equals("")) {
			parallelDegree = Integer.valueOf(parallelDegreeProperty);
		} else {
			parallelDegree = OracleData.getParallelProcesses(conn);
		}
		int rowsPerSlave = Integer.valueOf(getTestEnvProperty("it_num_rows")) / parallelDegree;
		try {
			long startTime = System.currentTimeMillis();
			OracleData.createTable(conn, fileName, parallelDegree, rowsPerSlave);
			LOG.info("Created and loaded table in " + ((System.currentTimeMillis() - startTime)/1000) + " seconds.");
		} catch (SQLException e) {
			if(e.getErrorCode()==955) {
				LOG.info("Table already exists - using existing data");
			} else {
				throw e;
			}
		}
	}
	
	public int importTable(String tableName, String targetDirectory) {
		setSqoopTargetDirectory(targetDirectory);
		return runImport(tableName);
	}
	
	public int exportTableFromTemplateTable(String templateTableName, String tableName, String targetDirectory) {
		setSqoopTargetDirectory(targetDirectory);
		return runExportFromTemplateTable(templateTableName, tableName);
	}
	
	public int compareTables(Connection conn, String table1, String table2) {
		try {
			int rowsDifferent = runCompareTables(conn, table1, table2);
			if (rowsDifferent == 0) {
				LOG.info("Tables match");
				return 0;
			} else {
				LOG.error("There are " + rowsDifferent + " rows that are different");
				return 1;
			}
		} catch(SQLException e) {
			LOG.error("Exception encountered",e);
			return 1;
		}
	}
	
	public int roundTrip(Connection conn, String fileName) throws Exception {
		OracleTableDefinition tableDefinition = new OracleTableDefinition(classLoader.getResource(fileName));
		String exportTableName = tableDefinition.getTableName() + "_EXP";
		
		createTable(conn, fileName);
		
		int ret = importTable(tableDefinition.getTableName(), tableDefinition.getTableName());
		if (ret != 0) {
			return ret;
		}
		
		ret = exportTableFromTemplateTable(tableDefinition.getTableName(), exportTableName, tableDefinition.getTableName());
		if (ret != 0) {
			return ret;
		}
		
		ret = compareTables(conn, tableDefinition.getTableName(), exportTableName);
		if (ret != 0) {
			return ret;
		}
		
		LOG.info("Round trip successful");
		return 0;
	}
	
	
	
	public static int run(String[] args) {
		try {
			CommandLineParser parser = new PosixParser();
			CommandLine line = parser.parse(options, args);
			String[] otherArgs = line.getArgs();
			
			OraOopCLI oraOopCLI = new OraOopCLI();
			
			if (line.getOptionValue("connect")!=null)
				oraOopCLI.setTestEnvProperty("oracle_url", line.getOptionValue("connect"));
			if (line.getOptionValue("username")!=null)
				oraOopCLI.setTestEnvProperty("oracle_username", line.getOptionValue("username"));
			if (line.getOptionValue("password")!=null)
				oraOopCLI.setTestEnvProperty("oracle_password", line.getOptionValue("password"));
			if (line.getOptionValue("parallel-degree")!=null)
				oraOopCLI.setTestEnvProperty("parallel_degree", line.getOptionValue("parallel-degree"));
			if (line.getOptionValue("num-rows")!=null)
				oraOopCLI.setTestEnvProperty("it_num_rows", line.getOptionValue("num-rows"));
			if (line.getOptionValue("num-mappers")!=null)
				oraOopCLI.setTestEnvProperty("num_mappers", line.getOptionValue("num-mappers"));
			if (line.getOptionValue("dfs-replication")!=null)
				oraOopCLI.setTestEnvProperty("dfs_replication", line.getOptionValue("dfs-replication"));
			
			Connection conn = oraOopCLI.getTestEnvConnection();
			
			String actionName = otherArgs[0];
			String[] tempArgs = new String[otherArgs.length-1];
			System.arraycopy(otherArgs, 1, tempArgs, 0, otherArgs.length-1);
			otherArgs = tempArgs;
			
			if("create-table".equalsIgnoreCase(actionName)) {
				if (otherArgs.length != 1) {
					LOG.error("Need to specify file name of table specification");
					return 1;
				}
				oraOopCLI.createTable(conn, otherArgs[0]);
				return 0;
			} else if("import-table".equalsIgnoreCase(actionName)) {
				if (otherArgs.length != 2) {
					LOG.error("Need to specify table name followed by target directory");
					return 1;
				}
				return oraOopCLI.importTable(otherArgs[0], otherArgs[1]);
			} else if("export-table-from-template".equalsIgnoreCase(actionName)) {
				if (otherArgs.length != 3) {
					LOG.error("Need to specify template table name, followed by target table name, followed by target directory");
					return 1;
				}
				return oraOopCLI.exportTableFromTemplateTable(otherArgs[0], otherArgs[1], otherArgs[2]);
			}  else if("compare-tables".equalsIgnoreCase(actionName)) {
				if (otherArgs.length != 2) {
					LOG.error("Need to specify two tables to compare");
					return 1;
				}
				return oraOopCLI.compareTables(conn, otherArgs[0], otherArgs[1]);
			} else if("round-trip".equalsIgnoreCase(actionName)) {
				if (otherArgs.length != 1) {
					LOG.error("Need to specify file name of table specification");
					return 1;
				}
				return oraOopCLI.roundTrip(conn, otherArgs[0]);
			}
			
			LOG.error("Did not specify valid action");
			return 1;
		} catch(Exception ex) {
			LOG.error("Exception encountered",ex);
			return 1;
		}
	}
	
	public static void main(String [] args) {
		if (args == null || args.length == 0) {
			printHelp();
			System.exit(0);
		}
		int ret = run(args);
		System.exit(ret);
	}
	
}