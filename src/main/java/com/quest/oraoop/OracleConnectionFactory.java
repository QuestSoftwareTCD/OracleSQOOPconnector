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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import oracle.jdbc.OracleConnection;

public class OracleConnectionFactory {

    protected static final OraOopLog LOG = OraOopLogFactory.getLog(OracleConnectionFactory.class.getName());

    public static Connection createOracleJdbcConnection(String jdbcDriverClassName
            ,String jdbcUrl
            ,String userName
            ,String password) 
        throws SQLException {
    
        Properties props = new Properties();
        props.put("user", userName);
        props.put("password", password);
        return createOracleJdbcConnection(jdbcDriverClassName, jdbcUrl, props);
    }
    
    public static Connection createOracleJdbcConnection(String jdbcDriverClassName
                                                       ,String jdbcUrl
                                                       ,Properties props) 
        throws SQLException {

        loadJdbcDriver(jdbcDriverClassName);
        Connection connection = createConnection(jdbcUrl, props);

        // Only OraOopDBRecordReader will call initializeOracleConnection(), as
        // we only need to initialize the session(s) prior to the mapper starting it's job.
        // i.e. We don't need to initialize the sessions in order to get the
        // table's data-files etc.
        
        // initializeOracleConnection(connection, conf);

        return connection;
    }

    private static void loadJdbcDriver(String jdbcDriverClassName) {

        try {
            Class.forName(jdbcDriverClassName);
        }
        catch (ClassNotFoundException ex) {
            String errorMsg = "Unable to load the jdbc driver class : " + jdbcDriverClassName;
            LOG.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
    }

    private static Connection createConnection(String jdbcUrl, Properties props) throws SQLException {

        try {
                return DriverManager.getConnection(jdbcUrl, props);
        }
        catch (SQLException ex) {
            String errorMsg = String.format("Unable to obtain a JDBC connection to the URL \"%s\" as user \"%s\": "
                                           ,jdbcUrl
                                           ,props.getProperty("user", "[null]"));
            LOG.error(errorMsg, ex);
            throw ex;
        }
    }

    public static void initializeOracleConnection(Connection connection, org.apache.hadoop.conf.Configuration conf) 
        throws SQLException {

        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        setSessionClientInfo(connection, conf);

        setJdbcFetchSize(connection, conf);

        executeOraOopSessionInitializationStatements(connection, conf);
    }

    protected static void setSessionClientInfo(Connection connection, org.apache.hadoop.conf.Configuration conf) {

        String sql = "";
        try {
            sql = "begin \n"+ 
                  "  dbms_application_info.set_module(module_name => '%s', action_name => '%s'); \n"+ 
                  "end;";

            String oracleSessionActionName = conf.get(OraOopConstants.ORACLE_SESSION_ACTION_NAME);

            sql = String.format(sql
                               ,OraOopConstants.ORACLE_SESSION_MODULE_NAME
                               ,oracleSessionActionName);

            Statement statement = connection.createStatement();
            statement.execute(sql);
            LOG.info("Initializing Oracle session with SQL :\n" + sql);
        }
        catch (Exception ex) {
            LOG.error(String.format("An error occurred while attempting to execute "+ 
                                    "the following Oracle session-initialization statement:"+ 
                                    "\n%s"+ 
                                    "\nError:" + "\n%s"
                                   ,sql
                                   ,ex.getMessage()));
        }
    }

    protected static void executeOraOopSessionInitializationStatements(Connection connection
                                                                      ,org.apache.hadoop.conf.Configuration conf) {

        List<String> statements = parseOraOopSessionInitializationStatements(conf);

        if (statements.size() == 0)
            LOG.warn(String.format("No Oracle 'session initialization' statements were found to execute.\n"+ 
                                   "Check that your %s and/or %s files are correctly installed in the ${SQOOP_HOME}/conf directory."
                                  ,OraOopConstants.ORAOOP_SITE_TEMPLATE_FILENAME
                                  ,OraOopConstants.ORAOOP_SITE_FILENAME));
        else {
            for (String statement : statements) {
                try {
                    connection.createStatement().execute(statement);
                    LOG.info("Initializing Oracle session with SQL : " + statement);
                }
                catch (Exception ex) {
                    LOG.error(String.format("An error occurred while attempting to execute "+ 
                                            "the following Oracle session-initialization statement:"+ 
                                            "\n%s"+ 
                                            "\nError:"+ 
                                            "\n%s"
                                           ,statement
                                           ,ex.getMessage()));
                }
            }
        }
    }

    protected static List<String> parseOraOopSessionInitializationStatements(org.apache.hadoop.conf.Configuration conf) {

        ArrayList<String> result = new ArrayList<String>();

        if (conf == null)
            throw new IllegalArgumentException("No configuration argument must be specified.");

        String sessionInitializationStatements = conf.get(OraOopConstants.ORAOOP_SESSION_INITIALIZATION_STATEMENTS);
        if (sessionInitializationStatements == null || 
            sessionInitializationStatements.isEmpty()) {
            // Nothing to do
        }
        else {

            String[] initializationStatements = sessionInitializationStatements.split(";");
            for (String initializationStatement : initializationStatements) {
                initializationStatement = initializationStatement.trim();
                if (initializationStatement != null && 
                   !initializationStatement.isEmpty() && 
                   !initializationStatement.startsWith(OraOopConstants.Oracle.ORACLE_SQL_STATEMENT_COMMENT_TOKEN)) {

                    LOG.debug(String.format("initializationStatement (quoted & pre-expression evaluation) = \"%s\""
                                           ,initializationStatement));

                    initializationStatement = OraOopUtilities.replaceConfigurationExpression(initializationStatement, conf);

                    result.add(initializationStatement);
                }
            }
        }
        return result;
    }

    protected static void setJdbcFetchSize(Connection connection, org.apache.hadoop.conf.Configuration conf) {

        int fetchSize = conf.getInt(OraOopConstants.ORACLE_ROW_FETCH_SIZE, OraOopConstants.ORACLE_ROW_FETCH_SIZE_DEFAULT);
        try {
            ((OracleConnection) connection).setDefaultRowPrefetch(fetchSize);

            String msg = "The Oracle connection has had its default row fetch size set to : " + fetchSize;
            if (fetchSize == OraOopConstants.ORACLE_ROW_FETCH_SIZE_DEFAULT)
                LOG.debug(msg);
            else
                LOG.info(msg);
        }
        catch (Exception ex) {
            LOG.warn(String.format("Unable to configure the DefaultRowPrefetch of the Oracle connection in %s."
                                  ,OraOopUtilities.getCurrentMethodName())
                                  ,ex);
        }

    }
}
