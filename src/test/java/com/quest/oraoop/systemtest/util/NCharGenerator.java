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

/**
 * Generates String test data. All generated characters will be encodable in UTF-8.
 * @author phall
 */
public class NCharGenerator extends TestDataGenerator<String>
{
  private int minLength;
  private int maxLength;

  /**
   * Create an NCharGenerator that will generate Strings between minLength and maxLength in length.
   * @param minLength Minimum length for generated strings
   * @param maxLength Maximum length for generated strings
   */
  public NCharGenerator(int minLength, int maxLength)
  {
    super();
    this.minLength = minLength;
    this.maxLength = maxLength;
  }

  @Override
  public String next()
  {
    int length = minLength + rng.nextInt(maxLength - minLength + 1);
    StringBuilder sb = new StringBuilder();
    while (sb.length() < length)
    {
      sb.append(Character.toChars(rng.nextInt(0x10FFFF)));
    }
    return sb.toString().substring(0, length);
  }

}
