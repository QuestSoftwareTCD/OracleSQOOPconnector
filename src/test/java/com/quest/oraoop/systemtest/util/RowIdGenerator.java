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

package com.quest.oraoop.systemtest.util;

import java.nio.charset.Charset;

import oracle.sql.ROWID;

/**
 * Generates ROWID test data. ROWIDs are represented by 18 ASCII encoded characters from the set A-Za-z0-9/+
 * 
 * Generated ROWIDs are unlikely to represent actual rows in any Oracle database, so should be used for
 * import/export tests only, and not used to reference data.
 * @author phall
 */
public class RowIdGenerator extends TestDataGenerator<ROWID>
{
  private static final String validChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789/+";
  private static final int length = 18;
  @Override
  public ROWID next()
  {
    StringBuffer sb = new StringBuffer();
    while (sb.length() < length)
    {
      sb.append(validChars.charAt(rng.nextInt(validChars.length())));
    }
    return new ROWID(sb.toString().getBytes(Charset.forName("US-ASCII")));
  }

}
