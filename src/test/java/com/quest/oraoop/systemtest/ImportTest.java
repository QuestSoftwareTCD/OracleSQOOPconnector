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

package com.quest.oraoop.systemtest;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;

import oracle.jdbc.OraclePreparedStatement;
import oracle.sql.STRUCT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Test;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.quest.oraoop.OraOopTestCase;
import com.quest.oraoop.systemtest.util.BigDecimalGenerator;
import com.quest.oraoop.systemtest.util.BinaryDoubleGenerator;
import com.quest.oraoop.systemtest.util.BinaryFloatGenerator;
import com.quest.oraoop.systemtest.util.BlobGenerator;
import com.quest.oraoop.systemtest.util.BytesGenerator;
import com.quest.oraoop.systemtest.util.CharGenerator;
import com.quest.oraoop.systemtest.util.FloatGenerator;
import com.quest.oraoop.systemtest.util.IntervalDaySecondGenerator;
import com.quest.oraoop.systemtest.util.IntervalYearMonthGenerator;
import com.quest.oraoop.systemtest.util.NCharGenerator;
import com.quest.oraoop.systemtest.util.RowIdGenerator;
import com.quest.oraoop.systemtest.util.TimestampGenerator;
import com.quest.oraoop.systemtest.util.URIGenerator;

/**
 * OraOop system tests of importing data from oracle to hadoop
 * 
 * @author phall
 */
public class ImportTest extends OraOopTestCase {
	
	private String sqoopGenLibDirectory = System.getProperty("user.dir") + "/target/tmp/lib";
	private String sqoopGenSrcDirectory = System.getProperty("user.dir") + "/target/tmp/src";
	private String sqoopTargetDirectory = "target/tmp/" + getTestEnvProperty("table_name");

	/**
	 * Generates pseudo-random test data across all supported data types in an
	 * Oracle database. Imports the data into Hadoop and compares with the data
	 * in Oracle.
	 * @throws Exception 
	 * 
	 * @throws Exception
	 */
	@Test
	public void importTest() throws Exception {
		// Generate test data in oracle
		final int numRows = Integer.valueOf(getTestEnvProperty("num_rows"));
		Connection conn = getTestEnvConnection();
		try {
			Statement s = conn.createStatement();
			try {
				s.executeUpdate("CREATE TABLE " + getTestEnvProperty("table_name")
						+ " (id NUMBER(10) PRIMARY KEY, bd BINARY_DOUBLE, bf BINARY_FLOAT, b BLOB, c CHAR(12), "
						+ "cl CLOB, d DATE, f FLOAT(126), l LONG, nc NCHAR(30), ncl NCLOB, n NUMBER(9,2), "
						+ "nvc NVARCHAR2(30), r ROWID, u URITYPE, iym INTERVAL YEAR(2) TO MONTH, "
						+ "ids INTERVAL DAY(2) TO SECOND(6), t TIMESTAMP(6), tz TIMESTAMP(6) WITH TIME ZONE, "
						+ "tltz TIMESTAMP(6) WITH LOCAL TIME ZONE, rawcol RAW(21))");
				BinaryDoubleGenerator bdg = new BinaryDoubleGenerator();
				BinaryFloatGenerator bfg = new BinaryFloatGenerator();
				BlobGenerator bg = new BlobGenerator(conn, 2 * 1024, 8 * 1024);
				CharGenerator cg = new CharGenerator(12, 12);
				CharGenerator clobg = new CharGenerator(2 * 1024, 8 * 1024);
				TimestampGenerator dateg = new TimestampGenerator(0);
				FloatGenerator fg = new FloatGenerator(126);
				CharGenerator lg = new CharGenerator(2 * 1024, 8 * 1024);
				NCharGenerator ncg = new NCharGenerator(30, 30);
				NCharGenerator nclobg = new NCharGenerator(2 * 1024, 8 * 1024);
				BigDecimalGenerator ng = new BigDecimalGenerator(9, 2);
				NCharGenerator nvcg = new NCharGenerator(1, 30);
				RowIdGenerator rg = new RowIdGenerator();
				URIGenerator ug = new URIGenerator();
				IntervalYearMonthGenerator iymg = new IntervalYearMonthGenerator(2);
				IntervalDaySecondGenerator idsg = new IntervalDaySecondGenerator(2, 6);
				TimestampGenerator tg = new TimestampGenerator(6);
				TimestampGenerator tzg = new TimestampGenerator(6);
				TimestampGenerator tltzg = new TimestampGenerator(6);
				BytesGenerator rawg = new BytesGenerator(21, 21);
				OraclePreparedStatement ps = (OraclePreparedStatement) conn
						.prepareStatement("INSERT INTO "
								+ getTestEnvProperty("table_name")
								+ " ( id, bd, bf, b, c, cl, d, f, nc, ncl, n, nvc, r, u, iym, ids, t, tz, tltz, rawcol ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, sys.UriFactory.getUri(?), ?, ?, ?, ?, ?, ? )");
				try {
					for (int i = 0; i < numRows; i++) {
						ps.setInt(1, i);
						ps.setBinaryDouble(2, bdg.next());
						ps.setBinaryFloat(3, bfg.next());
						ps.setBlob(4, bg.next());
						ps.setString(5, cg.next());
						ps.setString(6, clobg.next());
						ps.setTimestamp(7, dateg.next());
						ps.setBigDecimal(8, fg.next());
						ps.setString(9, ncg.next());
						ps.setString(10, nclobg.next());
						ps.setBigDecimal(11, ng.next());
						ps.setString(12, nvcg.next());
						ps.setRowId(13, rg.next());
						ps.setString(14, ug.next());
						ps.setString(15, iymg.next());
						ps.setString(16, idsg.next());
						ps.setTimestamp(17, tg.next());
						ps.setTimestamp(18, tzg.next());
						ps.setTimestamp(19, tltzg.next());
						ps.setBytes(20, rawg.next());
						ps.executeUpdate();
					}
				} finally {
					ps.close();
				}

				// Can't bind > 4000 bytes of data to LONG and LOB columns in the same statement, so do LONG by itself
				ps = (OraclePreparedStatement) conn.prepareStatement("UPDATE " + getTestEnvProperty("table_name")
						+ " SET l = ? WHERE id = ?");
				try {
					for (int i = 0; i < numRows; i++) {
						ps.setString(1, lg.next());
						ps.setInt(2, i);
						ps.executeUpdate();
					}
				} finally {
					ps.close();
				}

				try {
					// Import test data into hadoop
					String[] sqoopArgs = {"import",
							"--as-sequencefile",
							"--connect",
							getTestEnvProperty("oracle_url"),
							"--username",
							getTestEnvProperty("oracle_username"),
							"--password",
							getTestEnvProperty("oracle_password"),
							"--table",
							getTestEnvProperty("table_name"),
							"--target-dir",
							this.sqoopTargetDirectory,
							"--package-name",
							"com.quest.oraoop.systemtest.gen",
							"--bindir",
							this.sqoopGenLibDirectory,
							"--outdir",
							this.sqoopGenSrcDirectory
							};
					Configuration sqoopConf = new Configuration();
					sqoopConf.set("sqoop.connection.factories","com.quest.oraoop.OraOopManagerFactory");
					int retCode = Sqoop.runTool(sqoopArgs,sqoopConf);
					assertEquals("Return code should be 0", 0,retCode);

					// Add sqoop generated code to the classpath
					String sqoopGenJarPath = "file://" + this.sqoopGenLibDirectory + "/"
							+ getTestEnvProperty("table_name") + ".jar";
					URLClassLoader loader = new URLClassLoader(new URL[] { new URL(sqoopGenJarPath) }, getClass()
							.getClassLoader());
					Thread.currentThread().setContextClassLoader(loader);

					// Read test data from hadoop
					Configuration hadoopConf = new Configuration();
					FileSystem hdfs = FileSystem.get(hadoopConf);
					Path path = new Path(this.sqoopTargetDirectory);
					FileStatus[] statuses = hdfs.listStatus(path);
					int hadoopRecordCount = 0;
					for (FileStatus status : statuses) {
						if (status.getPath().getName().startsWith("part-m-")) {

							SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, status.getPath(), hadoopConf);
							LongWritable key = new LongWritable();
							@SuppressWarnings("unchecked")
							SqoopRecord value = ((Class<SqoopRecord>) reader.getValueClass()).getConstructor()
									.newInstance();
							ps = (OraclePreparedStatement) conn
									.prepareStatement("SELECT bd, bf, b, c, cl, d, f, l, nc, ncl, nvc, r, u, iym, ids, t, tz, tltz, rawcol FROM "
											+ getTestEnvProperty("table_name") + " WHERE id = ?");
							while (reader.next(key, value)) {
								// Compare test data from hadoop with data in oracle
								Map<String, Object> fields = value.getFieldMap();
								BigDecimal id = (BigDecimal) fields.get("ID");
								ps.setBigDecimal(1, id);
								ResultSet rs = ps.executeQuery();
								assertTrue("Did not find row with id " + id + " in oracle", rs.next());
								assertEquals("BinaryDouble did not match for row " + id, fields.get("BD"),
										rs.getDouble(1));
								assertEquals("BinaryFloat did not match for row " + id, fields.get("BF"),
										rs.getFloat(2));
								// LONG column needs to be read before BLOB column
								assertEquals("Long did not match for row " + id, fields.get("L"), rs.getString(8));
								BlobRef hadoopBlob = (BlobRef) fields.get("B");
								Blob oraBlob = rs.getBlob(3);
								assertTrue(
										"Blob did not match for row " + id,
										Arrays.equals(hadoopBlob.getData(),
												oraBlob.getBytes(1L, (int) oraBlob.length())));
								assertEquals("Char did not match for row " + id, fields.get("C"), rs.getString(4));
								ClobRef hadoopClob = (ClobRef) fields.get("CL");
								Clob oraClob = rs.getClob(5);
								assertEquals("Clob did not match for row " + id, hadoopClob.getData(),
										oraClob.getSubString(1, (int) oraClob.length()));
								assertEquals("Date did not match for row " + id, fields.get("D"), rs.getString(6));
								BigDecimal hadoopFloat = (BigDecimal) fields.get("F");
								BigDecimal oraFloat = rs.getBigDecimal(7);
								assertEquals("Float did not match for row " + id, hadoopFloat, oraFloat);
								assertEquals("NChar did not match for row " + id, fields.get("NC"), rs.getString(9));
								assertEquals("NClob did not match for row " + id, fields.get("NCL"), rs.getString(10));
								assertEquals("NVarChar did not match for row " + id, fields.get("NVC"),
										rs.getString(11));
								assertEquals("RowId did not match for row " + id, fields.get("R"), new String(rs
										.getRowId(12).getBytes()));
								STRUCT url = (STRUCT) rs.getObject(13); // TODO: Find a fix for this workaround
								String urlString = (String) url.getAttributes()[0];
								if (url.getSQLTypeName().equals("SYS.HTTPURITYPE")) {
									urlString = "http://" + urlString;
								} else if (url.getSQLTypeName().equals("SYS.DBURITYPE")) {
									urlString = "/ORADB" + urlString;
								}
								assertEquals("UriType did not match for row " + id, fields.get("U"), urlString);
								assertEquals("Interval Year to Month did not match for row " + id, fields.get("IYM"),
										rs.getString(14));
								String ids = (String) fields.get("IDS"); // Strip trailing zeros to match oracle format
								int lastNonZero = ids.length() - 1;
								while (ids.charAt(lastNonZero) == '0') {
									lastNonZero--;
								}
								ids = ids.substring(0, lastNonZero + 1);
								assertEquals("Interval Day to Second did not match for row " + id, ids,
										rs.getString(15));
								assertEquals("Timestamp did not match for row " + id, fields.get("T"), rs.getString(16));
								assertEquals("Timestamp with Time Zone did not match for row " + id, fields.get("TZ"),
										rs.getString(17));
								assertEquals("Timestamp with Local Time Zone did not match for row " + id,
										fields.get("TLTZ"), rs.getString(18));
								BytesWritable rawCol = (BytesWritable) fields.get("RAWCOL");
								byte[] rawColData = Arrays.copyOf(rawCol.getBytes(), rawCol.getLength());
								assertTrue("RAW did not match for row " + id,
										Arrays.equals(rawColData, rs.getBytes(19)));

								assertFalse("Found multiple rows with id " + id + " in oracle", rs.next());
								hadoopRecordCount++;
							}
						}
					}
					ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + getTestEnvProperty("table_name"));
					rs.next();
					int oracleRecordCount = rs.getInt(1);
					assertEquals("Number of records in Hadoop does not match number of records in oracle",
							hadoopRecordCount, oracleRecordCount);
					rs.close();
				} finally {
					// Delete test data from hadoop
					Configuration hadoopConf = new Configuration();
					FileSystem hdfs = FileSystem.get(hadoopConf);
					hdfs.delete(new Path(this.sqoopTargetDirectory), true);
					hdfs.delete(new Path(this.sqoopGenSrcDirectory), true);
					hdfs.delete(new Path(this.sqoopGenLibDirectory), true);
				}
			} finally {
				// Delete test data from oracle
				s.executeUpdate("DROP TABLE " + getTestEnvProperty("table_name"));
				s.close();
			}
			
		} finally {
			conn.close();
		}
	}
}
