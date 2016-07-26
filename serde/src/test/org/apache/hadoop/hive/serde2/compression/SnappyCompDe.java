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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.serde2.thrift.ColumnBuffer;
import org.apache.hive.service.rpc.thrift.TColumn;
import org.xerial.snappy.Snappy;

public class SnappyCompDe implements CompDe {

  /**
   * Initialize the plug-in by overlaying the input configuration map
   * onto the plug-in's default configuration.
   * 
   * @param config Overlay configuration map
   * 
   * @return The final configuration map is initialization was successful.
   *         `null` if initialization failed.
   */
  @Override
  public Map<String, String> init(Map<String, String> config) {
    return new HashMap<String, String>();
  }

  /**
   * Compress a set of columns
   * 
   * @param colSet
   * 
   * @return Bytes representing the compressed set.
   */
  @Override
  public byte[] compress(ColumnBuffer[] colSet) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    
    outputStream.write(colSet.length);
    for (int colNum = 0; colNum < colSet.length; colNum++) {
      try {
        byte[] compressedCol = colSet[colNum].toTColumn().getFieldValue().toString().getBytes();
        outputStream.write(compressedCol.length);
        outputStream.write(colSet[colNum].getType().toTType().getValue());
        outputStream.write(compressedCol);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return outputStream.toByteArray();
  }

  /**
   * Decompress a set of columns
   * 
   * @param compressedSet
   * 
   * @return The set of columns.
   */
  @Override
  public ColumnBuffer[] decompress(byte[] compressedSet) {
    ByteArrayInputStream input = new ByteArrayInputStream(compressedSet);
    int setSize = input.read();
    
    ColumnBuffer[] outputCols = new ColumnBuffer[setSize];
    for (int colNum = 0; colNum < setSize; colNum++) {
      int compressedSize = input.read();
      byte[] compressedCol = new byte[compressedSize];
      int columnType = input.read();
      input.read(compressedCol, 0, compressedSize);
      TColumn column = new TColumn();
      try {
        column.setFieldValue(columnType, (Object) Snappy.uncompress(compressedCol));
      } catch (IOException e) {
        e.printStackTrace();
      }
      outputCols[colNum] = new ColumnBuffer(column);
    }
    return outputCols;
  }

  /**
   * 
   * @return The plug-in name
   */
  @Override
  public String getName(){
    return "snappy";
  }

  /**
   * Provide a namespace for the plug-in
   * 
   * @return The vendor name
   */
  @Override
  public String getVendor() {
    return "snappy";
  }

}
