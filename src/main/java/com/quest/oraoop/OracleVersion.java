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

public class OracleVersion {
    public int    major;
    public int    minor;
    public int    version;
    public int    patch;
    public String banner;

    public OracleVersion(int major, int minor, int version, int patch, String banner) {

        this.major = major;
        this.minor = minor;
        this.version = version;
        this.patch = patch;
        this.banner = banner;
    }

    public boolean isGreaterThanOrEqualTo(int major, int minor, int version, int patch) {

        if (this.major > major)
            return true;

        if (this.major == major && this.minor > minor)
            return true;

        if (this.major == major && this.minor == minor && this.version > version)
            return true;

        if (this.major == major && this.minor == minor && this.version == version && this.patch >= patch)
            return true;

        return false;
    }
}
