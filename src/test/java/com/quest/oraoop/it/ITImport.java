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

package com.quest.oraoop.it;


import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import com.quest.oraoop.OraOopConstants;
import com.quest.oraoop.OraOopConstants.OraOopOracleDataChunkMethod;
import com.quest.oraoop.OraOopTestCase;

public class ITImport extends OraOopTestCase {
	
	@Test
	public void testProductImport() throws Exception {
		setSqoopTargetDirectory(getSqoopTargetDirectory() + "tst_product");
		createTable("table_tst_product.xml");
		
		try {
			int retCode = runImport("tst_product", getSqoopConf(), false);
			Assert.assertEquals("Return code should be 0", 0,retCode);
			
		} finally {
			cleanupFolders();
			closeTestEnvConnection();
		}
	}
	
	@Test
	public void testProductPartImport() throws Exception {
		setSqoopTargetDirectory(getSqoopTargetDirectory() + "tst_product_part");
		createTable("table_tst_product_part.xml");
		
		try {
			int retCode = runImport("tst_product_part", getSqoopConf(), false);
			Assert.assertEquals("Return code should be 0", 0,retCode);
			
		} finally {
			cleanupFolders();
			closeTestEnvConnection();
		}
	}
	
	@Test
	public void testProductPartImportPartitionChunk() throws Exception {
		setSqoopTargetDirectory(getSqoopTargetDirectory() + "tst_product_part");
		createTable("table_tst_product_part.xml");
		
    Configuration sqoopConf = getSqoopConf();
    sqoopConf.set(OraOopConstants.ORAOOP_ORACLE_DATA_CHUNK_METHOD, OraOopConstants.OraOopOracleDataChunkMethod.PARTITION.toString());
		
		try {
		  int retCode = runImport("tst_product_part", sqoopConf, false);
			Assert.assertEquals("Return code should be 0", 0,retCode);
			
		} finally {
			cleanupFolders();
			closeTestEnvConnection();
		}
	}
	
  @Test
  public void testProductPartImportSubset() throws Exception {
    setSqoopTargetDirectory(getSqoopTargetDirectory() + "tst_product_part");
    createTable("table_tst_product_part.xml");
    
    Configuration sqoopConf = getSqoopConf();
    sqoopConf.set(OraOopConstants.ORAOOP_ORACLE_DATA_CHUNK_METHOD, OraOopOracleDataChunkMethod.ROWID.toString());
    sqoopConf.set(OraOopConstants.ORAOOP_IMPORT_PARTITION_LIST, "tst_product_part_1,tst_product_part_2,\"tst_product_pa#rt_6\"");
    
    try {
      int retCode = runImport("tst_product_part", sqoopConf, false);
      Assert.assertEquals("Return code should be 0", 0,retCode);
      
    } finally {
      cleanupFolders();
      closeTestEnvConnection();
    }
  }
  
  @Test
  public void testProductPartImportSubsetPartitionChunk() throws Exception {
    setSqoopTargetDirectory(getSqoopTargetDirectory() + "tst_product_part");
    createTable("table_tst_product_part.xml");
    
    Configuration sqoopConf = getSqoopConf();
    sqoopConf.set(OraOopConstants.ORAOOP_ORACLE_DATA_CHUNK_METHOD, OraOopOracleDataChunkMethod.PARTITION.toString());
    sqoopConf.set(OraOopConstants.ORAOOP_IMPORT_PARTITION_LIST, "tst_product_part_1,tst_product_part_2,tst_product_part_3,\"tst_product_pa#rt_6\"");
    
    try {
      int retCode = runImport("tst_product_part", sqoopConf, false);
      Assert.assertEquals("Return code should be 0", 0,retCode);
      
    } finally {
      cleanupFolders();
      closeTestEnvConnection();
    }
  }
	
	@Test
	public void testProductSubPartImport() throws Exception {
		setSqoopTargetDirectory(getSqoopTargetDirectory() + "tst_product_subpart");
		createTable("table_tst_product_subpart.xml");
		
		try {
			int retCode = runImport("tst_product_subpart", getSqoopConf(), false);
			Assert.assertEquals("Return code should be 0", 0,retCode);
			
		} finally {
			cleanupFolders();
			closeTestEnvConnection();
		}
	}
	
	@Test
	public void testProductSubPartImportPartitionChunk() throws Exception {
		setSqoopTargetDirectory(getSqoopTargetDirectory() + "tst_product_subpart");
		createTable("table_tst_product_subpart.xml");
		
    Configuration sqoopConf = getSqoopConf();
    sqoopConf.set(OraOopConstants.ORAOOP_ORACLE_DATA_CHUNK_METHOD, OraOopConstants.OraOopOracleDataChunkMethod.PARTITION.toString());
		
		try {
			int retCode = runImport("tst_product_subpart", sqoopConf, false);
			Assert.assertEquals("Return code should be 0", 0,retCode);
			
		} finally {
			cleanupFolders();
			closeTestEnvConnection();
		}
	}
	
  @Test
  public void testProductSubPartImportSubset() throws Exception {
    setSqoopTargetDirectory(getSqoopTargetDirectory() + "tst_product_subpart");
    createTable("table_tst_product_subpart.xml");
    
    Configuration sqoopConf = getSqoopConf();
    sqoopConf.set(OraOopConstants.ORAOOP_ORACLE_DATA_CHUNK_METHOD, OraOopOracleDataChunkMethod.ROWID.toString());
    sqoopConf.set(OraOopConstants.ORAOOP_IMPORT_PARTITION_LIST, "TST_PRODUCT_PART_1,TST_PRODUCT_PART_2,TST_PRODUCT_PART_3,TST_PRODUCT_PART_4");
    
    try {
      int retCode = runImport("tst_product_subpart", sqoopConf, false);
      Assert.assertEquals("Return code should be 0", 0,retCode);
      
    } finally {
      cleanupFolders();
      closeTestEnvConnection();
    }
  }
  
  @Test
  public void testProductSubPartImportSubsetPartitionChunk() throws Exception {
    setSqoopTargetDirectory(getSqoopTargetDirectory() + "tst_product_subpart");
    createTable("table_tst_product_subpart.xml");

    Configuration sqoopConf = getSqoopConf();
    sqoopConf.set(OraOopConstants.ORAOOP_ORACLE_DATA_CHUNK_METHOD, OraOopConstants.OraOopOracleDataChunkMethod.PARTITION.toString());
    sqoopConf.set(OraOopConstants.ORAOOP_IMPORT_PARTITION_LIST, "TST_PRODUCT_PART_1,TST_PRODUCT_PART_2,TST_PRODUCT_PART_3");

    try {
      int retCode = runImport("tst_product_subpart", sqoopConf, false);
      Assert.assertEquals("Return code should be 0", 0,retCode);
      
    } finally {
      cleanupFolders();
      closeTestEnvConnection();
    }
  }

  @Test
  public void testProductImportConsistentRead() throws Exception {
    setSqoopTargetDirectory(getSqoopTargetDirectory() + "tst_product");
    createTable("table_tst_product.xml");

    Configuration sqoopConf = getSqoopConf();
    sqoopConf.setBoolean(OraOopConstants.ORAOOP_IMPORT_CONSISTENT_READ, true);
    
    try {
      int retCode = runImport("tst_product", sqoopConf, false);
      Assert.assertEquals("Return code should be 0", 0,retCode);
      
    } finally {
      cleanupFolders();
      closeTestEnvConnection();
    }
  }

}
