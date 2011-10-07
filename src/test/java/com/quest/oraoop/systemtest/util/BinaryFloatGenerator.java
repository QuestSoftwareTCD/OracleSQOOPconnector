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
 * Generates Float test data. Test data is distributed over the entire range of possible floats, including NaN,
 * positive and negative infinity and positive and negative zero.
 * @author phall
 */
public class BinaryFloatGenerator extends TestDataGenerator<Float>
{
  @Override
  public Float next()
  {
    return Float.intBitsToFloat(rng.nextInt());
  }

}
