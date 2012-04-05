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

import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import org.apache.commons.io.IOUtils;
import com.quest.oraoop.OraOopTestCase;

public abstract class OracleData {
	
	enum KeyType { PRIMARY, UNIQUE }
	
	private static ClassLoader classLoader;
	static {
	  classLoader = Thread.currentThread().getContextClassLoader();
	  if (classLoader == null) {
	    classLoader = OraOopTestCase.class.getClassLoader();
	  }
	}
	
	private static String getColumnList(List<OracleDataDefinition> columnList) {
		StringBuilder result = new StringBuilder();
		String delim = "";
		for (OracleDataDefinition column : columnList) {
			result.append(delim).append(column.getColumnName()).append(" ").append(column.getDataType());
			delim = ",\n";
		}
		return result.toString();
	}
	
	private static String getDataExpression(List<OracleDataDefinition> columnList) {
		StringBuilder result = new StringBuilder();
		for (OracleDataDefinition column : columnList) {
			result.append("l_ret_rec.").append(column.getColumnName()).append(" := ").append(column.getDataExpression()).append(";\n");
		}
		return result.toString();
	}
	
	private static void createPackageSpec(Connection conn,
			OracleTableDefinition tableDefinition)
					    		 throws Exception {
		String pkgSql = IOUtils.toString(classLoader.getResource("pkg_tst_product_gen.psk").openStream());
		pkgSql = pkgSql.replaceAll("\\$COLUMN_LIST", getColumnList(tableDefinition.getColumnList()));
		pkgSql = pkgSql.replaceAll("\\$TABLE_NAME", tableDefinition.getTableName());
		PreparedStatement stmt = conn.prepareStatement(pkgSql);
		stmt.execute();
	}
	
	private static void createPackageBody(Connection conn,
			OracleTableDefinition tableDefinition)
		     					 throws Exception {
		String pkgSql = IOUtils.toString(classLoader.getResource("pkg_tst_product_gen.pbk").openStream());
		pkgSql = pkgSql.replaceAll("\\$COLUMN_LIST", getColumnList(tableDefinition.getColumnList()));
		pkgSql = pkgSql.replaceAll("\\$TABLE_NAME", tableDefinition.getTableName());
		pkgSql = pkgSql.replaceAll("\\$DATA_EXPRESSION_LIST", getDataExpression(tableDefinition.getColumnList()));
		pkgSql = pkgSql.replaceAll("\\$PARTITION_CLAUSE", tableDefinition.getPartitionClause());
		PreparedStatement stmt = conn.prepareStatement(pkgSql);
		stmt.execute();
	}
	
	private static void createKey(Connection conn,
			KeyType keyType,
			OracleTableDefinition tableDefinition)
								throws Exception {
		List<String> columns = null;
		switch(keyType) {
			case PRIMARY: columns = tableDefinition.getPrimaryKeyColumns();
				break;
			case UNIQUE: columns = tableDefinition.getUniqueKeyColumns();
				break;
		}
		if (columns != null && columns.size() > 0) {
			StringBuilder keyColumnList = new StringBuilder();
			String delim = "";
			for (String column : columns) {
				keyColumnList.append(delim).append(column);
				delim = ",";
			}
			String keySql = "alter table $TABLE_NAME add constraint $TABLE_NAME_" +
							((keyType==KeyType.PRIMARY) ? "pk primary key" : "uk unique") +
							"($PK_COLUMN_LIST) " + 
							"using index (create unique index $TABLE_NAME_" + 
							((keyType==KeyType.PRIMARY) ? "pk" : "uk") +
							" on $TABLE_NAME($PK_COLUMN_LIST) " +
							"parallel nologging)";
			keySql = keySql.replaceAll("\\$TABLE_NAME", tableDefinition.getTableName());
			keySql = keySql.replaceAll("\\$PK_COLUMN_LIST", keyColumnList.toString());
			PreparedStatement stmt = conn.prepareStatement(keySql);
			stmt.execute();
		}
	}
	
	public static int getParallelProcesses(Connection conn) throws Exception {
		PreparedStatement stmt = conn.prepareStatement(
				"SELECT cc.value value" + "\n" +
				"FROM" + "\n" +
				"  (SELECT to_number(value) value" + "\n" +
				"  FROM v$parameter" + "\n" +
				"  WHERE name='parallel_max_servers'" + "\n" +
				"  ) pms," + "\n" +
				"  (SELECT to_number(value) value" + "\n" +
				"  FROM v$parameter" + "\n" +
				"  WHERE name='parallel_threads_per_cpu'" + "\n" +
				"  ) ptpc," + "\n" +
				"  (SELECT to_number(value) value FROM v$parameter WHERE name='cpu_count'" + "\n" +
				"  ) cc");
		ResultSet res = stmt.executeQuery();
		res.next();
		return res.getInt(1);
	}
	
	public static void createTable(Connection conn,
			OracleTableDefinition tableDefinition,
			int parallelDegree,
			int rowsPerSlave) throws Exception {
		createPackageSpec(conn, tableDefinition);
		createPackageBody(conn, tableDefinition);

		CallableStatement procStmt = conn.prepareCall("begin pkg_odg_" + tableDefinition.getTableName() + ".prc_load_table(?,?); end;");
		procStmt.setInt(1, parallelDegree);
		procStmt.setInt(2, rowsPerSlave);
		procStmt.execute();
		
		createKey(conn, KeyType.PRIMARY, tableDefinition);
		createKey(conn, KeyType.UNIQUE, tableDefinition);
	}
	
	public static void createTable(Connection conn,
			String fileName,
			int parallelDegree,
			int rowsPerSlave) throws Exception {
		URL file = classLoader.getResource(fileName);
		OracleTableDefinition tableDefinition = new OracleTableDefinition(file);
		createTable(conn, tableDefinition, parallelDegree, rowsPerSlave);
	}

}
