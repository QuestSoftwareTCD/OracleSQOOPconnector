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
import java.net.URL;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import oracle.jdbc.OracleConnection;

public abstract class OraOopTestCase {
	
	protected static OraOopLog LOG = OraOopLogFactory.getLog(OraOopTestCase.class.getName());
	
	protected final String sqoopGenLibDirectory = System.getProperty("user.dir") + "/target/tmp/lib";
	protected final String sqoopGenSrcDirectory = System.getProperty("user.dir") + "/target/tmp/src";
	protected final String sqoopTargetDirectory = "target/tmp/";
	
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
	
	private final Properties conf = new Properties();
	
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
	
	protected String getTestEnvProperty(String name) {
		return this.conf.getProperty(name);
	}
	
	protected OracleConnection getTestEnvConnection() throws SQLException {
		return (OracleConnection) DriverManager.getConnection(this.getTestEnvProperty("oracle_url"),this.getTestEnvProperty("oracle_username"),this.getTestEnvProperty("oracle_password"));
	}
	
	protected Configuration getSqoopConf() {
		Configuration sqoopConf = new Configuration();
		sqoopConf.set("sqoop.connection.factories","com.quest.oraoop.OraOopManagerFactory");
		return sqoopConf;
	}

}
