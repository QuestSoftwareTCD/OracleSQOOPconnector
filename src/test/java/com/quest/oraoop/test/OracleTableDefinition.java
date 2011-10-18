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

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

public class OracleTableDefinition {
	
	private String tableName;
	private List<OracleDataDefinition> columnList = new ArrayList<OracleDataDefinition>();
	private List<String> primaryKeyColumns = new ArrayList<String>();
	private List<String> uniqueKeyColumns = new ArrayList<String>();
	
	public List<String> getUniqueKeyColumns() {
		return uniqueKeyColumns;
	}
	public void setUniqueKeyColumns(List<String> uniqueKeyColumns) {
		this.uniqueKeyColumns = uniqueKeyColumns;
	}
	public List<String> getPrimaryKeyColumns() {
		return primaryKeyColumns;
	}
	public void setPrimaryKeyColumns(List<String> primaryKeyColumns) {
		this.primaryKeyColumns = primaryKeyColumns;
	}
	public List<OracleDataDefinition> getColumnList() {
		return columnList;
	}
	public void setColumnList(List<OracleDataDefinition> columnList) {
		this.columnList = columnList;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public OracleTableDefinition() {
		
	}
	
	@SuppressWarnings("unchecked")
	public OracleTableDefinition(URL url) {
		try {
			XMLConfiguration conf = new XMLConfiguration();
			conf.setDelimiterParsingDisabled(true);
			conf.load(url);
			
			tableName = conf.getString("name");
			
			List<?> columns = conf.configurationsAt("columns.column");
			for(Iterator<?> it = columns.iterator(); it.hasNext(); ) {
				HierarchicalConfiguration sub = (HierarchicalConfiguration) it.next();
				this.columnList.add(new OracleDataDefinition(sub.getString("name"), sub.getString("dataType"), sub.getString("dataExpression")));
			}
			
			primaryKeyColumns = conf.getList("primaryKeyColumns.primaryKeyColumn");
			uniqueKeyColumns = conf.getList("uniqueKeyColumns.uniqueKeyColumn");
		} catch (ConfigurationException e) {
			throw new RuntimeException("Could not load table configuration",e);
		}
	}

}
