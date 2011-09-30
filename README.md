Quest® Data Connector for Oracle and Hadoop
===========================================

Quest® Data Connector for Oracle and Hadoop is an optional plugin to Sqoop. It facilitates the movement of data between Oracle and Hadoop.

Development Prerequisites
-------------------------

To check the source code out you will need [Git](http://git-scm.com/).

You will need to have the following in order to compile the project:

* [Oracle Java SE 6 JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
* [Apache Maven](http://maven.apache.org/)
* [Oracle Database 11g Release 2 (11.2.0.2.0) JDBC Drivers](http://www.oracle.com/technetwork/database/enterprise-edition/jdbc-112010-090769.html)

You will also need to install the Oracle JDBC driver into your local maven repository with the following command:

	mvn install:install-file -Dfile=ojdbc6.jar -Dpackaging=jar -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=11.2.0.2.0

Getting Started
---------------

1. To get a copy of the source code:

	git clone http://github.com/QuestSoftwareTCD/OracleSQOOPconnector.git

2. To compile and generate jar archive:

	cd OracleSQOOPConnector
	mvn package

Using Eclipse
-------------

1. Generate the Eclipse project with the following:

	mvn eclipse:eclipse

2. Import the project into the Eclipse workspace.
3. Create a new Run Configuration for the project.
4. Specify com.cloudera.sqoop.Sqoop as the main class.
5. Set the working directory - by default data will be created in the working directory.
6. You can now pass program arguments as normal, for example:

	import -Dsqoop.connection.factories=com.quest.oraoop.OraOopManagerFactory --connect jdbc:oracle:thin:@//hostname:port:service --username username --password password --table oracle\_table --target-dir target\_dir

This will run Sqoop with the default configuration - you can set the SQOOP\_CONF\_DIR environment variable if needed. It will pick up the oraoop-site-template.xml and oraoop-site.xml configuration files by default.
