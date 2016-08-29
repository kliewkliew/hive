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


package org.apache.hadoop.hive.serde2.compression;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.serde2.thrift.ColumnBuffer;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface CompDe {

  /**
   * Initialize the plug-in by overlaying the input configuration map
   * onto the plug-in's default configuration.
   *
   * @param config Overlay configuration map.
   *
   * @return True if initialization was successful.
   */
  public boolean init(Map<String, String> config);

  /**
   * Return the configuration settings of the CompDe after initialization.
   *
   * @return
   */
  public Map<String, String> getConfig();

  /**
   * Compress a set of columns.
   *
   * @param colSet
   *
   * @return ByteBuffer representing the compressed set.
   */
  public ByteBuffer compress(ColumnBuffer[] colSet);

  /**
   * Decompress a set of columns from a ByteBuffer and update the position of the buffer.
   *
   * @param input A ByteBuffer with `position` indicating the starting point of the compressed chunk. 
   * @param chunkSize The length of the compressed chunk to be decompressed from the input buffer.
   *
   * @return The set of columns.
   */
  public ColumnBuffer[] decompress(ByteBuffer input, int chunkSize);

  /**
   *
   * @return The plug-in name.
   */
  public String getName();

  /**
   * Provide a namespace for the plug-in.
   *
   * @return The vendor name.
   */
  public String getVendor();
}
