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

package com.quest.oraoop.systemtest.util;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;

import oracle.sql.BLOB;

/**
 * Generates Blob test data.
 * @author phall
 */
public class BlobGenerator extends TestDataGenerator<Blob>
{
  private Connection conn;
  private int minBytes;
  private int maxBytes;

  /**
   * Create a generator that will generate BLOBs with length varying between minBytes and maxBytes.
   * @param conn Oracle connection to use when creating BLOBs
   * @param minBytes Minimum number of bytes in generated BLOBs
   * @param maxBytes Maximum number of bytes in generated BLOBs
   */
  public BlobGenerator(Connection conn, int minBytes, int maxBytes)
  {
    super();
    this.conn = conn;
    this.minBytes = minBytes;
    this.maxBytes = maxBytes;
  }

  @Override
  public Blob next()
  {
    try
    {
      BLOB blob = BLOB.createTemporary(conn, false, BLOB.DURATION_SESSION);

      int blobSize = (int)(rng.nextDouble() * (maxBytes - minBytes) + minBytes);
      byte[] blobData = new byte[blobSize];
      rng.nextBytes(blobData);

      // blob.setBytes(blobData);

      OutputStream os = blob.setBinaryStream(1);
      InputStream is = new ByteArrayInputStream(blobData);
      byte[] buffer = new byte[blob.getBufferSize()];
      int bytesRead = 0;
      while ((bytesRead = is.read(buffer)) != -1)
      {
        os.write(buffer, 0, bytesRead);
      }
      os.close();
      is.close();

      return blob;
    }
    catch(Exception e)
    {
      throw new RuntimeException(e);
    }
  }

}
