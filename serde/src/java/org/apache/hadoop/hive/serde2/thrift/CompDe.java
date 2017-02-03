/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.hadoop.hive.serde2.thrift;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
/**
 * CompDe is the interface used to compress or decompress a set of ColumnBuffer.
 * It is used to compress results in ThriftJDBCBinarySerDe to send to remote
 * clients.
 *
 */
public interface CompDe {

  /**
   * Table descriptor keys.
   */
  public final static String confName = "compde.name";
  public final static String confVersion = "compde.version";
  public final static String confParams = "compde.params";

  /**
   * Negotiate the server and client plug-in parameters.
   * parameters.
   * @param serverParams The server's default parameters for this plug-in.
   * @param clientParams The client's requested parameters for this plug-in.
   * @throws Exception if the plug-in failed to initialize.
   */
  public Map<String,String> getParams(
      final Map<String,String> serverParams,
      final Map<String,String> clientParams)
          throws Exception;

  /**
   * Initialize the plug-in with parameters.
   * @param params
   * @throws Exception if the plug-in failed to initialize.
   */
  public void init(final Map<String,String> params)
      throws Exception;

  /**
   * Compress a set of columns.
   * @param colSet The set of columns to be compressed.
   * @return ByteBuffer representing the compressed set.
   * @throws Exception
   */
  public ByteBuffer compress(final ColumnBuffer[] colSet)
      throws Exception;

  /**
   * Decompress a set of columns from a ByteBuffer and update the position of
   * the buffer.
   * @param input     A ByteBuffer with `position` indicating the starting point
   *                  of the compressed chunk.
   * @param chunkSize The length of the compressed chunk to be decompressed from
   *                  the input buffer.
   * @return The set of columns.
   * @throws Exception
   */
  public ColumnBuffer[] decompress(final ByteBuffer input, int chunkSize)
      throws Exception;

  /**
   * Provide a namespace for the plug-in.
   * @return The vendor name.
   */
  public String getVendor();

  /**
   * Provide a name for the plug-in.
   * @return The plug-in name.
   */
  public String getName();

  /**
   * Provide the version of the plug-in.
   * @return The plug-in version.
   */
  public String getVersion();
}
