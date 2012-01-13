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

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


public class OracleTableDefinition {
	
	private String tableName;
	private List<OracleDataDefinition> columnList = new ArrayList<OracleDataDefinition>();
	private List<String> primaryKeyColumns = new ArrayList<String>();
	private List<String> uniqueKeyColumns = new ArrayList<String>();
	private String partitionClause;
	
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
	public String getPartitionClause() {
		return partitionClause == null ? "" : partitionClause;
	}
	public void setPartitionClause(String partitionClause) {
		this.partitionClause = partitionClause;
	}
	public OracleTableDefinition() {
		
	}

	public OracleTableDefinition(URL url) {
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.parse(new File(url.toURI()));
			
			Element table = doc.getDocumentElement();
			this.tableName = table.getElementsByTagName("name").item(0).getChildNodes().item(0).getNodeValue();
			NodeList columns = table.getElementsByTagName("column");
			for (int i = 0; i < columns.getLength(); i++) {
				Node columnNode = columns.item(i);
				if (columnNode.getNodeType() == Node.ELEMENT_NODE) {
					Element columnElement = (Element) columnNode;
					String name = columnElement.getElementsByTagName("name").item(0).getChildNodes().item(0).getNodeValue();
					String dataType = columnElement.getElementsByTagName("dataType").item(0).getChildNodes().item(0).getNodeValue();
					String dataExpression = columnElement.getElementsByTagName("dataExpression").item(0).getChildNodes().item(0).getNodeValue();
					this.columnList.add(new OracleDataDefinition(name,dataType,dataExpression));
				}
			}
			
			NodeList primaryKeyColumns = table.getElementsByTagName("primaryKeyColumn");
			for (int i = 0; i < primaryKeyColumns.getLength(); i++) {
				Node primaryKeyColumnNode = primaryKeyColumns.item(i);
				if (primaryKeyColumnNode.getNodeType() == Node.ELEMENT_NODE) {
					Element primaryKeyColumnElement = (Element) primaryKeyColumnNode;
					this.primaryKeyColumns.add(primaryKeyColumnElement.getChildNodes().item(0).getNodeValue());
				}
			}
			
			NodeList uniqueKeyColumns = table.getElementsByTagName("uniqueKeyColumn");
			for (int i = 0; i < uniqueKeyColumns.getLength(); i++) {
				Node uniqueKeyColumnNode = uniqueKeyColumns.item(i);
				if (uniqueKeyColumnNode.getNodeType() == Node.ELEMENT_NODE) {
					Element uniqueKeyColumnElement = (Element) uniqueKeyColumnNode;
					this.uniqueKeyColumns.add(uniqueKeyColumnElement.getChildNodes().item(0).getNodeValue());
				}
			}
			
			Node partitionClauseNode = table.getElementsByTagName("partitionClause").item(0);
			if (partitionClauseNode!=null) {
				this.partitionClause = partitionClauseNode.getChildNodes().item(0).getNodeValue();
			}
		} catch(Exception e) {
			throw new RuntimeException("Could not load table configuration",e);
		}
	}

}
