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

package com.quest.oraoop;

import org.apache.commons.logging.LogFactory;

public class OraOopLogFactory {

    public interface OraOopLog2 {

        boolean getCacheLogEntries();
        void setCacheLogEntries(boolean value);
        String getLogEntries();
        void clearCache();
    }

    public static OraOopLog getLog(Class<?> clazz) {

        return OraOopLogFactory.getLog(clazz.getName());
    }

    public static OraOopLog getLog(String logName) {

        return new OraOopLog(LogFactory.getLog(logName));
    }

}
