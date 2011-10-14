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

package com.quest.oraoop.test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.quest.oraoop.OraOopTestCase;

public abstract class OracleData {
	
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
											 String tableName,
					     List<OracleDataDefinition> columnList)
					    		 throws Exception {
		String pkgSql = IOUtils.toString(classLoader.getResource("pkg_tst_product_gen.psk").openStream());
		pkgSql = pkgSql.replaceAll("\\$COLUMN_LIST", getColumnList(columnList));
		pkgSql = pkgSql.replaceAll("\\$TABLE_NAME", tableName);
		PreparedStatement stmt = conn.prepareStatement(pkgSql);
		stmt.execute();
	}
	
	private static void createPackageBody(Connection conn,
			 								 String tableName,
		     			 List<OracleDataDefinition> columnList)
		     					 throws Exception {
		String pkgSql = IOUtils.toString(classLoader.getResource("pkg_tst_product_gen.pbk").openStream());
		pkgSql = pkgSql.replaceAll("\\$COLUMN_LIST", getColumnList(columnList));
		pkgSql = pkgSql.replaceAll("\\$TABLE_NAME", tableName);
		pkgSql = pkgSql.replaceAll("\\$DATA_EXPRESSION_LIST", getDataExpression(columnList));
		PreparedStatement stmt = conn.prepareStatement(pkgSql);
		stmt.execute();
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
			String tableName,
			List<OracleDataDefinition> columnList,
			int parallelDegree,
			int rowsPerSlave) throws Exception {
		createPackageSpec(conn, tableName, columnList);
		createPackageBody(conn, tableName, columnList);

		CallableStatement procStmt = conn.prepareCall("begin pkg_odg_" + tableName + ".prc_load_table(?,?); end;");
		procStmt.setInt(1, parallelDegree);
		procStmt.setInt(2, rowsPerSlave);
		procStmt.execute();
	}

}
