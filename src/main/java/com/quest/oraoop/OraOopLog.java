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

public class OraOopLog implements org.apache.commons.logging.Log, OraOopLogFactory.OraOopLog2 {

    private org.apache.commons.logging.Log LOG;
    private StringBuilder cache;

    public OraOopLog(org.apache.commons.logging.Log log) {

        this.LOG = log;
    }

    @Override
    public void debug(Object message) {

        if (cacheLogEntry(message))
            return;

        LOG.debug(message);

    }

    @Override
    public void debug(Object message, Throwable t) {

        if (cacheLogEntry(message))
            return;

        LOG.debug(message, t);
    }

    @Override
    public void error(Object message) {

        if (cacheLogEntry(message))
            return;

        LOG.error(message);
    }

    @Override
    public void error(Object message, Throwable t) {

        if (cacheLogEntry(message))
            return;

        LOG.error(message, t);
    }

    @Override
    public void fatal(Object message) {

        if (cacheLogEntry(message))
            return;

        LOG.fatal(message);
    }

    @Override
    public void fatal(Object message, Throwable t) {

        if (cacheLogEntry(message))
            return;

        LOG.fatal(message, t);
    }

    @Override
    public void info(Object message) {

        if (cacheLogEntry(message))
            return;

        LOG.info(message);
    }

    @Override
    public void info(Object message, Throwable t) {

        if (cacheLogEntry(message))
            return;

        LOG.info(message, t);
    }

    @Override
    public boolean isDebugEnabled() {

        return LOG.isDebugEnabled();
    }

    @Override
    public boolean isErrorEnabled() {

        return LOG.isErrorEnabled();
    }

    @Override
    public boolean isFatalEnabled() {

        return LOG.isFatalEnabled();
    }

    @Override
    public boolean isInfoEnabled() {

        return LOG.isInfoEnabled();
    }

    @Override
    public boolean isTraceEnabled() {

        return LOG.isTraceEnabled();
    }

    @Override
    public boolean isWarnEnabled() {

        return LOG.isWarnEnabled();
    }

    @Override
    public void trace(Object message) {

        LOG.trace(message);
        cacheLogEntry(message);
    }

    @Override
    public void trace(Object message, Throwable t) {

        if (cacheLogEntry(message))
            return;

        LOG.trace(message, t);
    }

    @Override
    public void warn(Object message) {

        if (cacheLogEntry(message))
            return;

        LOG.warn(message);
    }

    @Override
    public void warn(Object message, Throwable t) {

        if (cacheLogEntry(message))
            return;

        LOG.warn(message, t);
    }

    @Override
    public boolean getCacheLogEntries() {

        return (this.cache != null);
    }

    @Override
    public String getLogEntries() {

        if (this.cache != null)
            return this.cache.toString();
        else
            return "";
    }

    @Override
    public void setCacheLogEntries(boolean value) {

        if (getCacheLogEntries() && !value)
            this.cache = null;
        else if (!getCacheLogEntries() && value)
            this.cache = new StringBuilder();
    }

    @Override
    public void clearCache() {

        if (getCacheLogEntries())
            this.cache = new StringBuilder();
    }

    private boolean cacheLogEntry(Object message) {

        boolean result = getCacheLogEntries();

        if (result && message != null)
            this.cache.append(message.toString());

        return result;
    }

}

// }
