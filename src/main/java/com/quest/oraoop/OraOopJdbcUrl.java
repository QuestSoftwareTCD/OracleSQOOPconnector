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

import java.lang.reflect.Method;
import java.util.Properties;

import com.cloudera.sqoop.util.JdbcUrl;
import com.quest.oraoop.OraOopUtilities.JdbcOracleThinConnection;
import com.quest.oraoop.OraOopUtilities.JdbcOracleThinConnectionParsingError;

public class OraOopJdbcUrl {

    private static final OraOopLog LOG = OraOopLogFactory.getLog(OraOopJdbcUrl.class.getName());
    private String jdbcConnectString;
    
    public OraOopJdbcUrl(String jdbcConnectString) {
        
        if (jdbcConnectString == null)
            throw new IllegalArgumentException("The jdbcConnectionString argument must not be null.");

        if (jdbcConnectString.isEmpty())
            throw new IllegalArgumentException("The jdbcConnectionString argument must not be empty.");
        
        this.jdbcConnectString = jdbcConnectString;
    }
    
    public JdbcOracleThinConnection parseJdbcOracleThinConnectionString() 
        throws JdbcOracleThinConnectionParsingError {

    /*
     * http://wiki.oracle.com/page/JDBC
     * 
     * There are different flavours of JDBC connections for Oracle, including: 
     *  Thin 
     *      E.g. jdbc:oracle:thin:@mel601643.melquest.dev.mel.au.qsft:1521:bnorac01
     * 
     *      A pure Java driver used on the client side that does not need an Oracle client installation. 
     *      It is recommended that you use this driver unless you need support for non-TCP/IP networks 
     *      because it provides for maximum portability and performance.
     * 
     * 
     * Oracle Call Interface driver (OCI). 
     *      E.g. jdbc:oracle:oci8:@bnorac01.world //<- "bnorac01.world" is a TNS entry
     * 
     *      This uses the Oracle client installation libraries and interfaces. If you want to support 
     *      connection pooling or client side caching of requests, use this driver. You will also need 
     *      this driver if you are using transparent application failover (TAF) from your application 
     *      as well as strong authentication like Kerberos and PKI certificates.
     * 
     * JDBC-ODBC bridge. 
     *      E.g. jdbc:odbc:mydatabase //<- "mydatabase" is an ODBC data source.
     * 
     *      This uses the ODBC driver in Windows to connect to the database.
     */

    String hostName = null;
    int port = 0;
    String sid = null;
    String service = null;

    String jdbcUrl = this.jdbcConnectString.trim();
    
    // If there are any parameters included at the end of the connection URL, let's remove them now...
    int paramsIdx = jdbcUrl.indexOf("?");
    if(paramsIdx > -1) {
        jdbcUrl = jdbcUrl.substring(0, paramsIdx);
    }
    
    /*
     * The format of an Oracle jdbc URL is one of:
     *      jdbc:oracle:<driver-type>:@<host>:<port>:<sid>
     *      jdbc:oracle:<driver-type>:@<host>:<port>/<service>
     *      jdbc:oracle:<driver-type>:@<host>:<port>/<service>?<parameters>
     *      jdbc:oracle:<driver-type>:@//<host>:<port>/<service>
     *      jdbc:oracle:<driver-type>:@//<host>:<port>/<service>?<parameters>
     */
   
    // Split the URL on its ":" characters...
    String[] jdbcFragments = jdbcUrl.trim().split(":");
    
    // Clean up each fragment of the URL...
    for (int idx = 0; idx < jdbcFragments.length; idx++)
        jdbcFragments[idx] = jdbcFragments[idx].trim();        
    
    // Check we can proceed...
    if (jdbcFragments.length < 5 || jdbcFragments.length > 6)
        throw new JdbcOracleThinConnectionParsingError(String.format("There should be 5 or 6 colon-separated pieces of data in the JDBC URL, such as:\n"+ 
                                                                     "\tjdbc:oracle:<driver-type>:@<host>:<port>:<sid>\n"+ 
                                                                     "\tjdbc:oracle:<driver-type>:@<host>:<port>/<service>\n"+
                                                                     "\tjdbc:oracle:<driver-type>:@<host>:<port>/<service>?<parameters>\n"+
                                                                     "The JDBC URL specified was:\n"+ 
                                                                     "%s\n"+ 
                                                                     "which contains %d pieces of colon-separated data."
                                                                    ,this.jdbcConnectString
                                                                    ,jdbcFragments.length));
    
    // jdbc
    if (!jdbcFragments[0].equalsIgnoreCase("jdbc"))
        throw new JdbcOracleThinConnectionParsingError("The first item in the colon-separated JDBC URL must be \"jdbc\".");

    // jdbc:oracle
    if (!jdbcFragments[1].equalsIgnoreCase("oracle"))
        throw new JdbcOracleThinConnectionParsingError("The second item in the colon-separated JDBC URL must be \"oracle\".");

    // jdbc:oracle:thin
    if (!jdbcFragments[2].equalsIgnoreCase("thin"))
        throw new JdbcOracleThinConnectionParsingError(String.format("The Oracle \"thin\" JDBC driver is not being used.\n"+ 
                                                                     "The third item in the colon-separated JDBC URL must be \"thin\", not \"%s\"."
                                                                    ,jdbcFragments[2]));        
    
    // jdbc:oracle:thin:@<host>
    hostName = jdbcFragments[3];
    if (hostName.isEmpty() || hostName.equalsIgnoreCase("@"))
        throw new JdbcOracleThinConnectionParsingError("The fourth item in the colon-separated JDBC URL (the host name) must not be empty.");

    if (!hostName.startsWith("@"))
        throw new JdbcOracleThinConnectionParsingError("The fourth item in the colon-separated JDBC URL (the host name) must a prefixed with the \"@\" character.");

    String portStr = "";
    switch(jdbcFragments.length) {
        case 6:
            // jdbc:oracle:<driver-type>:@<host>:<port>:<sid>
            portStr = jdbcFragments[4];
            sid = jdbcFragments[5];
            break;
            
        case 5:
            // jdbc:oracle:<driver-type>:@<host>:<port>/<service>
            String[] portAndService = jdbcFragments[4].split("/");
            if(portAndService.length != 2)
                throw new JdbcOracleThinConnectionParsingError("The fifth colon-separated item in the JDBC URL (<port>/<service>) must contain two items separated by a \"/\".");
            portStr = portAndService[0].trim();
            service = portAndService[1].trim();
            break;
    }
    
    if (portStr.isEmpty())
        throw new JdbcOracleThinConnectionParsingError("The fifth item in the colon-separated JDBC URL (the port) must not be empty.");

    try {
        port = Integer.parseInt(portStr);
    }
    catch (NumberFormatException ex) {
        throw new JdbcOracleThinConnectionParsingError(String.format("The fifth item in the colon-separated JDBC URL (the port) must be a valid number.\n"+ 
                                                                     "\"%s\" could not be parsed as an integer."
                                                                    ,portStr));
    }

    if (port <= 0)
        throw new JdbcOracleThinConnectionParsingError(String.format("The fifth item in the colon-separated JDBC URL (the port) must be greater than zero.\n"+ 
                                                                     "\"%s\" was specified."
                                                                    ,portStr));        
    
    if(sid == null && service == null)
        throw new JdbcOracleThinConnectionParsingError("The JDBC URL does not contain a SID or SERVICE. The URL should look like one of these:\n"+
                                                        "\tjdbc:oracle:<driver-type>:@<host>:<port>:<sid>\n"+ 
                                                        "\tjdbc:oracle:<driver-type>:@<host>:<port>/<service>\n"+
                                                        "\tjdbc:oracle:<driver-type>:@<host>:<port>/<service>?<parameters>\n"+
                                                        "\tjdbc:oracle:<driver-type>:@//<host>:<port>/<service>\n"+
                                                        "\tjdbc:oracle:<driver-type>:@<host>:<port>/<service>?<parameters>");

    JdbcOracleThinConnection result = new JdbcOracleThinConnection(hostName.replaceFirst("^[@][/]{0,2}", "") // <- Remove the "@" prefix of the hostname
                                                                    , port
                                                                    , sid
                                                                    , service);

    return result;
}    

    public boolean connectionStringContainsProperties()
    {
        return this.jdbcConnectString.indexOf("?") > -1;
    }
    
    private String getConnectionStringProperties() {
        
        return this.jdbcConnectString.substring(this.jdbcConnectString.indexOf("?"));
    }
    
    public String getConnectionUrl() {
    
        // If there are no parameters included in the jdbc connection url, then
        // just return it...
        if(!connectionStringContainsProperties())
            return this.jdbcConnectString;
        
        // Simply return the portion of the connect string before any parameters.
        // We'll be returning a string like:
        //  jdbc:oracle:<driver-type>:@<host>:<port>:<sid>
        //    or
        //  jdbc:oracle:<driver-type>:@<host>:<port>/<service>
        String[] jdbcConnectStrFragments = this.jdbcConnectString.split("\\?");
        return jdbcConnectStrFragments[0];
    }
    
    private Properties createProperties(String userName, String password) {

        Properties result = new Properties();
        result.put("user", userName);
        result.put("password", password);
        return result;
    }
    
    public static Method findGetConnectionPropertiesMethod() {
        
        Method getConnectionProperties = null;
        try {
            getConnectionProperties = JdbcUrl.class.getDeclaredMethod("getConnectionProperties", String.class, String.class, String.class); 
        }
        catch (SecurityException e) { }
        catch (NoSuchMethodException e) {  }
        
        return getConnectionProperties;
    }
    
    public Properties getConnectionProperties(String userName, String password) {

        if(!connectionStringContainsProperties())
            return this.createProperties(userName, password);
            
        Method getConnectionProperties = findGetConnectionPropertiesMethod();
        if(getConnectionProperties == null) {
            
            LOG.info("The parameters included in the jdbc URL cannot be processed, as you're not using Sqoop 1.3 or have "+
                     "not installed patch SQOOP-172.");            
            
            return this.createProperties(userName, password);
        }

        /*
         * If the jdbc URL contains the name of an Oracle service, its format will be:
         * jdbc:oracle:<driver-type>:@<host>:<port>/<service>?<parameters>
         * This is a well-formed URL that can be parsed.
         * 
         * However, if the jdbc URL contains an Oracle sid, its format will be:
         * jdbc:oracle:<driver-type>:@<host>:<port>:<sid>?<parameters>
         * This is not a well-formed URL.
         */

        Properties result = null;
        try {
            String wellFormedJdbcUrlWithProperties = "jdbc:oracle:thin:@host:1521/service" + this.getConnectionStringProperties();
            result = (Properties) getConnectionProperties.invoke(null, wellFormedJdbcUrlWithProperties, userName, password);
            return result;
        }
        catch (Exception ex) {
            LOG.warn("The parameters included in the jdbc URL could not be processed properly. They will be ignored.", ex);
            return this.createProperties(userName, password);
        }
    }
    
}
