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

public class TestConstants {

	public static final String SQL_TABLE =  
	
"	WITH sqltable AS "+
"    ( "+
"       SELECT executions, rows_processed, fetches, "+
"              ROUND (rows_processed / executions, 2) AS rows_per_exec, "+
"              ROUND (rows_processed / fetches, 2) AS rows_per_fetch, "+
"              ROUND (LEAST (  ROUND (rows_processed / fetches, 2) "+
"                            / LEAST (rows_processed / executions, 10), "+
"                            1 "+
"                           ), "+
"                     2 "+
"                    ) batch_efficiency, "+
"              sql_text, u.username parsing_schema_name, buffer_gets, disk_reads, "+
"              cpu_time/1000 cpu_time, elapsed_time/1000 elapsed_time, hash_value sql_id, child_number "+ 
"         FROM v$sql s join all_users u on (u.user_id=s.parsing_user_id) "+
"        WHERE fetches > 0 AND executions > 0 AND rows_processed > 0 "+
"          AND parsing_schema_id <> 0 AND sql_text like 'select%dba_objects' )"+
"SELECT   sql_id, child_number, array_wastage, rows_processed, fetches, rows_per_exec, "+
"        rows_per_fetch, parsing_schema_name, buffer_gets, disk_reads, "+
"        cpu_time, elapsed_time, sql_text,executions "+
"   FROM (SELECT sql_id, " +
"                child_number, " +
"                rows_processed * (1 - batch_efficiency) array_wastage, "+
"                rows_processed, " +
"                fetches, " +
"                rows_per_exec, " +
"                rows_per_fetch, "+
"                sql_text, " +
"                parsing_schema_name, " +
"                buffer_gets, " +
"                disk_reads, "+
"                cpu_time, " +
"                elapsed_time, "+
//"                RANK () OVER (ORDER BY rows_processed * (1 - batch_efficiency) DESC) ranking, "+
"                executions "+
"           FROM sqltable) ";
//"  WHERE ranking < 10000  "+
//"ORDER BY ranking ";
	
	
}
