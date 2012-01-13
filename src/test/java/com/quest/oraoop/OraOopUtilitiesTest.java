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

import java.io.IOException;
import java.sql.SQLException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.quest.oraoop.OraOopUtilities;

public class OraOopUtilitiesTest extends OraOopTestCase {

//	@Test
//	public void testGetCurrentMethodName() {
//		fail("Not yet implemented");
//	}
//
//	@Test
//	public void testDecodeOracleTableName() {
//		fail("Not yet implemented");
//	}

	@Test
	public void testdecodeOracleTableName() {
		
		System.out.print(String.format("\t%s - Start...", OraOopUtilities.getCurrentMethodName()));
		
		//org.apache.hadoop.conf.Configuration conf = new Configuration();

		OracleTable context = null; 
		
		// These are the possibilities for double-quote location...
		//	table
		//	"table"
		//	schema.table
		// 	schema."table"
		//	"schema".table
		//	"schema"."table"
		
		// table
		context = OraOopUtilities.decodeOracleTableName("oraoop", "junk", null);
		Assert.assertEquals(context.getSchema(), "ORAOOP");
		Assert.assertEquals(context.getName(), "JUNK");

		// "table"
		context = OraOopUtilities.decodeOracleTableName("oraoop", "\"Junk\"", null);
		Assert.assertEquals(context.getSchema(), "ORAOOP");
		Assert.assertEquals(context.getName(), "Junk");
		
		// schema.table
		context = OraOopUtilities.decodeOracleTableName("oraoop", "targusr.junk", null);
		Assert.assertEquals(context.getSchema(), "TARGUSR");
		Assert.assertEquals(context.getName(), "JUNK");

		// schema."table"
		context = OraOopUtilities.decodeOracleTableName("oraoop", "targusr.\"Junk\"", null);
		Assert.assertEquals(context.getSchema(), "TARGUSR");
		Assert.assertEquals(context.getName(), "Junk");
		
		// "schema".table
		context = OraOopUtilities.decodeOracleTableName("oraoop", "\"Targusr\".junk", null);
		Assert.assertEquals(context.getSchema(), "Targusr");
		Assert.assertEquals(context.getName(), "JUNK");		
		
		// "schema"."table"
		String inputStr = "\"Targusr\".\"Junk\"";
		context = OraOopUtilities.decodeOracleTableName("oraoop", inputStr, null);
		Assert.assertEquals(context.getSchema(), "Targusr");
		Assert.assertEquals(context.getName(), "Junk");	
		
		// Test for "." within schema...
		context = OraOopUtilities.decodeOracleTableName("oraoop", "\"targ.usr\".junk", null);
		Assert.assertEquals(context.getSchema(), "targ.usr");
		Assert.assertEquals(context.getName(), "JUNK");	
		
		// Test for "." within table...		
		context = OraOopUtilities.decodeOracleTableName("oraoop", "targusr.\"junk.tab.with.dots\"", null);
		Assert.assertEquals(context.getSchema(), "TARGUSR");
		Assert.assertEquals(context.getName(), "junk.tab.with.dots");
		
		// Test for "." within schema and within table...		
		context = OraOopUtilities.decodeOracleTableName("oraoop", "\"targ.usr\".\"junk.tab.with.dots\"", null);
		Assert.assertEquals(context.getSchema(), "targ.usr");
		Assert.assertEquals(context.getName(), "junk.tab.with.dots");
		
		System.out.println("Finished");
	}

	@Test
	public void testgetCurrentMethodName() {
		
		System.out.print(String.format("\t%s - Start...", OraOopUtilities.getCurrentMethodName()));
		
		String actual = OraOopUtilities.getCurrentMethodName();
		String expected = "testgetCurrentMethodName()";
		
		Assert.assertEquals(expected, actual);
		
		System.out.println("Finished");
	}	
	
	@Test
	public void testgenerateDataChunkId() {

		System.out.print(String.format("\t%s - Start...", OraOopUtilities.getCurrentMethodName()));
		
		String expected;
		String actual;
		
		expected = "1_1";
		actual = OraOopUtilities.generateDataChunkId(1, 1);
		Assert.assertEquals(expected, actual);			
		
		expected = "1234_99";
		actual = OraOopUtilities.generateDataChunkId(1234, 99);
		Assert.assertEquals(expected, actual);			
				
		System.out.println("Finished");
	}	
	
	@Test
	public void testgetDuplicatedStringArrayValues() {

		System.out.print(String.format("\t%s - Start...", OraOopUtilities.getCurrentMethodName()));
		
		try {
			OraOopUtilities.getDuplicatedStringArrayValues(null, false);
			Assert.fail("An IllegalArgumentException should be been thrown.");
		}
		catch(IllegalArgumentException ex) {
			// This is what we want to happen. 
		}
		
		String[] duplicates = null;

		duplicates = OraOopUtilities.getDuplicatedStringArrayValues(new String[] {}, false);
		Assert.assertEquals(0, duplicates.length);
			
		duplicates = OraOopUtilities.getDuplicatedStringArrayValues(new String[] {"a", "b", "c"}, false);
		Assert.assertEquals(0, duplicates.length);
		
		duplicates = OraOopUtilities.getDuplicatedStringArrayValues(new String[] {"a", "A", "b"}, false);
		Assert.assertEquals(0, duplicates.length);

		duplicates = OraOopUtilities.getDuplicatedStringArrayValues(new String[] {"a", "A", "b"}, true);
		Assert.assertEquals(1, duplicates.length);
		Assert.assertEquals("A", duplicates[0]);
		
		duplicates = OraOopUtilities.getDuplicatedStringArrayValues(new String[] {"A", "a", "b"}, true);
		Assert.assertEquals(1, duplicates.length);
		Assert.assertEquals("a", duplicates[0]);		

		duplicates = OraOopUtilities.getDuplicatedStringArrayValues(new String[] {"A", "a", "b", "A"}, false);
		Assert.assertEquals(1, duplicates.length);
		Assert.assertEquals("A", duplicates[0]);
		
		duplicates = OraOopUtilities.getDuplicatedStringArrayValues(new String[] {"A", "a", "b", "A"}, true);
		Assert.assertEquals(2, duplicates.length);
		Assert.assertEquals("a", duplicates[0]);			
		Assert.assertEquals("A", duplicates[1]);
		
		duplicates = OraOopUtilities.getDuplicatedStringArrayValues(new String[] {"A", "a", "b", "A", "A"}, true);
		Assert.assertEquals(2, duplicates.length);
		Assert.assertEquals("a", duplicates[0]);			
		Assert.assertEquals("A", duplicates[1]);
		
		System.out.println("Finished");
	}
	
	@Test
	public void testgetFullExceptionMessage() {
		
		System.out.print(String.format("\t%s - Start...", OraOopUtilities.getCurrentMethodName()));
		
		try {
			
			try {
				try {
					throw new IOException("lorem ipsum!");
				}
				catch(IOException ex) {
					throw new SQLException("dolor sit amet", ex);
				}
			}
			catch(SQLException ex) {
				throw new RuntimeException("consectetur adipisicing elit", ex);
			}
				
		}
		catch(Exception ex) {
			String msg = OraOopUtilities.getFullExceptionMessage(ex);
			if(!msg.contains("IOException") ||
			   !msg.contains("lorem ipsum!"))
				Assert.fail("Inner exception text has not been included in the message");
			if(!msg.contains("SQLException") ||
			   !msg.contains("dolor sit amet"))
				Assert.fail("Inner exception text has not been included in the message");
			if(!msg.contains("RuntimeException") ||
			   !msg.contains("consectetur adipisicing elit"))
				Assert.fail("Outer exception text has not been included in the message");				
		}
		
		System.out.println("Finished");
	}
	
	@Test
	public void testGetOraOopOracleDataChunkMethod() {
		try {
			OraOopUtilities.getOraOopOracleDataChunkMethod(null);
			Assert.fail("An IllegalArgumentException should be been thrown.");
		} catch(IllegalArgumentException ex) {
			// This is what we want to happen. 
		}
		
		OraOopConstants.OraOopOracleDataChunkMethod dataChunkMethod;
		Configuration conf = new Configuration();
		
		//Check the default is ROWID
		dataChunkMethod = OraOopUtilities.getOraOopOracleDataChunkMethod(conf);
		Assert.assertEquals(OraOopConstants.OraOopOracleDataChunkMethod.ROWID, dataChunkMethod);
		
		//Invalid value specified
		OraOopUtilities.LOG.setCacheLogEntries(true);
		OraOopUtilities.LOG.clearCache();
		conf.set(OraOopConstants.ORAOOP_ORACLE_DATA_CHUNK_METHOD, "loremipsum");
		dataChunkMethod = OraOopUtilities.getOraOopOracleDataChunkMethod(conf);
		String logText = OraOopUtilities.LOG.getLogEntries();
		OraOopUtilities.LOG.setCacheLogEntries(false);
		if(!logText.toLowerCase().contains("loremipsum"))
			Assert.fail("The LOG should inform the user they've selected an invalid data chunk method - and what that was.");
		Assert.assertEquals("Should have used the default value",OraOopConstants.ORAOOP_ORACLE_DATA_CHUNK_METHOD_DEFAULT, dataChunkMethod);
		
		//Valid value specified
		conf.set(OraOopConstants.ORAOOP_ORACLE_DATA_CHUNK_METHOD, "partition");
		dataChunkMethod = OraOopUtilities.getOraOopOracleDataChunkMethod(conf);
		Assert.assertEquals(OraOopConstants.OraOopOracleDataChunkMethod.PARTITION, dataChunkMethod);
	}
	
	@Test
	public void testgetOraOopOracleBlockToSplitAllocationMethod() {

		System.out.print(String.format("\t%s - Start...", OraOopUtilities.getCurrentMethodName()));
		
		// Invalid arguments test...
		try {
			OraOopUtilities.getOraOopOracleBlockToSplitAllocationMethod(null, OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.RANDOM);
			Assert.fail("An IllegalArgumentException should be been thrown.");
		}
		catch(IllegalArgumentException ex) {
			// This is what we want to happen. 
		}
		
		OraOopConstants.OraOopOracleBlockToSplitAllocationMethod allocationMethod;
		org.apache.hadoop.conf.Configuration conf = new Configuration();

		// No configuration property - and RANDOM used by default...
		allocationMethod = OraOopUtilities.getOraOopOracleBlockToSplitAllocationMethod(conf, OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.RANDOM);
		Assert.assertEquals(OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.RANDOM, allocationMethod);

		// No configuration property - and SEQUENTIAL used by default...
		allocationMethod = OraOopUtilities.getOraOopOracleBlockToSplitAllocationMethod(conf, OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.SEQUENTIAL);
		Assert.assertEquals(OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.SEQUENTIAL, allocationMethod);
		
		// An invalid property value specified...
		OraOopUtilities.LOG.setCacheLogEntries(true);
		OraOopUtilities.LOG.clearCache();
		conf.set(OraOopConstants.ORAOOP_ORACLE_BLOCK_TO_SPLIT_ALLOCATION_METHOD, "loremipsum");
		allocationMethod = OraOopUtilities.getOraOopOracleBlockToSplitAllocationMethod(conf, OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.SEQUENTIAL);
		String logText = OraOopUtilities.LOG.getLogEntries();
		OraOopUtilities.LOG.setCacheLogEntries(false);
		if(!logText.toLowerCase().contains("loremipsum"))
			Assert.fail("The LOG should inform the user they've selected an invalid allocation method - and what that was.");
		
		if(!logText.contains("ROUNDROBIN or SEQUENTIAL or RANDOM"))
			Assert.fail("The LOG should inform the user what the valid choices are.");
		
		// An valid property value specified...
		conf.set(OraOopConstants.ORAOOP_ORACLE_BLOCK_TO_SPLIT_ALLOCATION_METHOD, "sequential");		
		allocationMethod = OraOopUtilities.getOraOopOracleBlockToSplitAllocationMethod(conf, OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.SEQUENTIAL);
		Assert.assertEquals(OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.SEQUENTIAL, allocationMethod);
		
		System.out.println("Finished");
	}
	
	@Test
	public void testgetOraOopTableImportWhereClauseLocation() {

		System.out.print(String.format("\t%s - Start...", OraOopUtilities.getCurrentMethodName()));
		
		// Invalid arguments test...
		try {
			OraOopUtilities.getOraOopTableImportWhereClauseLocation(null, OraOopConstants.OraOopTableImportWhereClauseLocation.SPLIT);
			Assert.fail("An IllegalArgumentException should be been thrown.");
		}
		catch(IllegalArgumentException ex) {
			// This is what we want to happen. 
		}
		
		OraOopConstants.OraOopTableImportWhereClauseLocation location;
		org.apache.hadoop.conf.Configuration conf = new Configuration();

		// No configuration property - and SPLIT used by default...
		location = OraOopUtilities.getOraOopTableImportWhereClauseLocation(conf, OraOopConstants.OraOopTableImportWhereClauseLocation.SPLIT);
		Assert.assertEquals(OraOopConstants.OraOopTableImportWhereClauseLocation.SPLIT, location);

		// An invalid property value specified...
		OraOopUtilities.LOG.setCacheLogEntries(true);
		OraOopUtilities.LOG.clearCache();
		conf.set(OraOopConstants.ORAOOP_TABLE_IMPORT_WHERE_CLAUSE_LOCATION, "loremipsum");
		location = OraOopUtilities.getOraOopTableImportWhereClauseLocation(conf, OraOopConstants.OraOopTableImportWhereClauseLocation.SPLIT);
		String logText = OraOopUtilities.LOG.getLogEntries();
		OraOopUtilities.LOG.setCacheLogEntries(false);
		if(!logText.toLowerCase().contains("loremipsum"))
			Assert.fail("The LOG should inform the user they've selected an invalid where-clause-location - and what that was.");
		
		if(!logText.contains("SUBSPLIT or SPLIT"))
			Assert.fail("The LOG should inform the user what the valid choices are.");
		
		// An valid property value specified...
		conf.set(OraOopConstants.ORAOOP_TABLE_IMPORT_WHERE_CLAUSE_LOCATION, "split");		
		location = OraOopUtilities.getOraOopTableImportWhereClauseLocation(conf, OraOopConstants.OraOopTableImportWhereClauseLocation.SUBSPLIT);
		Assert.assertEquals(OraOopConstants.OraOopTableImportWhereClauseLocation.SPLIT, location);
		
		System.out.println("Finished");
	}	
	
	@Test
	public void testpadLeft() {
		
		System.out.print(String.format("\t%s - Start...", OraOopUtilities.getCurrentMethodName()));
		
		String expected = "   a";
		String actual = OraOopUtilities.padLeft("a", 4);
		Assert.assertEquals(expected, actual);
		
		expected = "abcd";
		actual = OraOopUtilities.padLeft("abcd", 3);
		Assert.assertEquals(expected, actual);		
		
		System.out.println("Finished");
	}
	
	@Test
	public void testpadRight() {
		
		System.out.print(String.format("\t%s - Start...", OraOopUtilities.getCurrentMethodName()));
		
		String expected = "a   ";
		String actual = OraOopUtilities.padRight("a", 4);
		Assert.assertEquals(expected, actual);
		
		expected = "abcd";
		actual = OraOopUtilities.padRight("abcd", 3);
		Assert.assertEquals(expected, actual);		
		
		System.out.println("Finished");
	}	
	
	@Test
	public void testReplaceConfigurationExpression() {
		
		System.out.print(String.format("\t%s - Start...", OraOopUtilities.getCurrentMethodName()));
		
		org.apache.hadoop.conf.Configuration conf = new Configuration();
		
		// Default value used...
		String actual = OraOopUtilities.replaceConfigurationExpression("alter session set timezone = '{oracle.sessionTimeZone|GMT}';", conf);
		String expected = "alter session set timezone = 'GMT';";
		Assert.assertEquals("OraOop configuration expression failure.", expected, actual);
		
		// Configuration property value exists...
		conf.set("oracle.sessionTimeZone", "Africa/Algiers");
		actual = OraOopUtilities.replaceConfigurationExpression("alter session set timezone = '{oracle.sessionTimeZone|GMT}';", conf);
		expected = "alter session set timezone = 'Africa/Algiers';";
		Assert.assertEquals("OraOop configuration expression failure.", expected, actual);

		// Multiple properties in one expression...
		conf.set("expr1", "1");
		conf.set("expr2", "2");
		conf.set("expr3", "3");
		conf.set("expr4", "4");
		actual = OraOopUtilities.replaceConfigurationExpression("set {expr1}={expr2};", conf);
		expected = "set 1=2;";
		Assert.assertEquals("OraOop configuration expression failure.", expected, actual);

		actual = OraOopUtilities.replaceConfigurationExpression("set {expr4|0}={expr5|5};", conf);
		expected = "set 4=5;";
		Assert.assertEquals("OraOop configuration expression failure.", expected, actual);
		
		System.out.println("Finished");
	}	
	
	@Test
	public void testStackContainsClass() {
		
		System.out.print(String.format("\t%s - Start...", OraOopUtilities.getCurrentMethodName()));
		
		if(OraOopUtilities.stackContainsClass("lorem.ipsum.dolor"))
			Assert.fail("There's no way the stack actually contains this!");
		
		String expected = "com.quest.oraoop.OraOopUtilitiesTest";
		if(!OraOopUtilities.stackContainsClass(expected))
			Assert.fail("The stack should contain the class:" + expected);
		
		System.out.println("Finished");
	}
	
	@Test
	public void testGetImportHint() {
		org.apache.hadoop.conf.Configuration conf = new Configuration();
		
		String hint = OraOopUtilities.getImportHint(conf);
		Assert.assertEquals("Default import hint","/*+ NO_INDEX(t) */ ", hint);
		
		conf.set("oraoop.import.hint", "NO_INDEX(t) SCN_ASCENDING");
		hint = OraOopUtilities.getImportHint(conf);
		Assert.assertEquals("Changed import hint","/*+ NO_INDEX(t) SCN_ASCENDING */ ", hint);
		
		conf.set("oraoop.import.hint", "       ");
		hint = OraOopUtilities.getImportHint(conf);
		Assert.assertEquals("Whitespace import hint","", hint);
		
		conf.set("oraoop.import.hint", "");
		hint = OraOopUtilities.getImportHint(conf);
		Assert.assertEquals("Blank import hint","", hint);

	}
}
