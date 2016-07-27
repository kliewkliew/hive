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
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.serde2.thrift.ColumnBuffer;
import org.apache.hive.service.rpc.thrift.*;
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
    try {
      for (int colNum = 0; colNum < colSet.length; colNum++) {
        
        outputStream.write(colSet[colNum].getType().toTType().getValue());
        
        switch (TTypeId.findByValue(colSet[colNum].getType().toTType().getValue())) {
        case BOOLEAN_TYPE: {
          TBoolColumn column = colSet[colNum].toTColumn().getBoolVal();

          byte[] compressedNulls = Snappy.compress(column.getNulls());
          outputStream.write(compressedNulls.length);
          outputStream.write(compressedNulls);

          List<Boolean> bools = column.getValues();
          BitSet bsBools = new BitSet(bools.size());
          for (int rowNum = 0; rowNum < bools.size(); rowNum++) {
            bsBools.set(rowNum, bools.get(rowNum));
          }

          writePrimitiveBytes(column.getNulls(), outputStream);
          writePrimitiveBytes(bsBools.toByteArray(), outputStream);

          break;
        }
        case TINYINT_TYPE: {
          TByteColumn column = colSet[colNum].toTColumn().getByteVal();
          writePrimitiveBytes(column.getNulls(), outputStream);
          writeBoxedPrimitive(column.getValues(), outputStream);
          break;
        }
        case SMALLINT_TYPE: {
          TI16Column column = colSet[colNum].toTColumn().getI16Val();
          writePrimitiveBytes(column.getNulls(), outputStream);
          writeBoxedPrimitive(column.getValues(), outputStream);
          break;
        }
        case INT_TYPE: {
          TI32Column column = colSet[colNum].toTColumn().getI32Val();
          writePrimitiveBytes(column.getNulls(), outputStream);
          writeBoxedPrimitive(column.getValues(), outputStream);
          break;
        }
        case BIGINT_TYPE: {
          TI64Column column = colSet[colNum].toTColumn().getI64Val();
          writePrimitiveBytes(column.getNulls(), outputStream);
          writeBoxedPrimitive(column.getValues(), outputStream);
          break;
        }
        case DOUBLE_TYPE: {
          TDoubleColumn column = colSet[colNum].toTColumn().getDoubleVal();
          writePrimitiveBytes(column.getNulls(), outputStream);
          writeBoxedPrimitive(column.getValues(), outputStream);
          break;
        }
        case BINARY_TYPE: {
          TBinaryColumn column = colSet[colNum].toTColumn().getBinaryVal();

          // Flatten the data for Snappy
          int[] rowSizes = new int[column.getValuesSize()]; 
          ByteArrayOutputStream flattenedData = new ByteArrayOutputStream();

          for (int rowNum = 0; rowNum < column.getValuesSize(); rowNum++) {
            byte[] row = column.getValues().get(rowNum).asCharBuffer().toString().getBytes();
            rowSizes[rowNum] = row.length;
            flattenedData.write(row);
          }

          // Write nulls bitmap
          writePrimitiveBytes(column.getNulls(), outputStream);

          // Write the list of row sizes
          writePrimitiveInts(rowSizes, outputStream);

          // Write the flattened data
          writePrimitiveBytes(flattenedData.toByteArray(), outputStream);

          break;
        }
        case STRING_TYPE: {
          TStringColumn column = colSet[colNum].toTColumn().getStringVal();

          // Flatten the data for Snappy
          int[] rowSizes = new int[column.getValuesSize()]; 
          ByteArrayOutputStream flattenedData = new ByteArrayOutputStream();

          for (int rowNum = 0; rowNum < column.getValuesSize(); rowNum++) {
            byte[] row = column.getValues().get(rowNum).getBytes();
            rowSizes[rowNum] = row.length;
            flattenedData.write(row);
          }

          // Write nulls bitmap
          writePrimitiveBytes(column.getNulls(), outputStream);

          // Write the list of row sizes
          writePrimitiveInts(rowSizes, outputStream);

          // Write the flattened data
          writePrimitiveBytes(flattenedData.toByteArray(), outputStream);

          break;
        }
        default:
          throw new IllegalStateException("Unrecognized column type");
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return outputStream.toByteArray();
  }
  
  /**
   * Write the length, and data to the output stream.
   * 
   * @param boxedVals A List of boxed Java-primitives.
   * @param unwrappedClass The primitive type (must be accepted as input to Snappy.compress).
   * @param outputStream
   * @throws IOException 
   */
  private <T extends Number> void writeBoxedPrimitive(List<T> boxedVals,  ByteArrayOutputStream outputStream) throws IOException {
    byte[] compressedVals = new byte[0];
    // Arrays of boxed primitives must be unboxed before Snappy will accept it as input 
    // but there are no generic methods for unboxing arrays so we use if-else.
    Class<?> listType = boxedVals.get(0).getClass();
    if (listType.isInstance(Byte.class)) {
      compressedVals = Snappy.compress(ArrayUtils.toPrimitive((Byte[]) boxedVals.toArray()));
    }
    else if (listType.isInstance(Short.class)) {
      compressedVals = Snappy.compress(ArrayUtils.toPrimitive((Short[]) boxedVals.toArray()));
    }
    else if (listType.isInstance(Integer.class)) {
      compressedVals = Snappy.compress(ArrayUtils.toPrimitive((Integer[]) boxedVals.toArray()));
    }
    else if (listType.isInstance(Long.class)) {
      compressedVals = Snappy.compress(ArrayUtils.toPrimitive((Long[]) boxedVals.toArray()));
    }
    else if (listType.isInstance(Double.class)) {
      compressedVals = Snappy.compress(ArrayUtils.toPrimitive((Double[]) boxedVals.toArray()));
    }
    else {
      throw new IllegalStateException("Unrecognized column type");
    }
    writeBytes(compressedVals, outputStream);
  }

  /**
   * Java generics do not support primitive types
   * @param primitives
   * @param outputStream
   * @throws IOException 
   */
  private void writePrimitiveBytes(byte[] primitives, ByteArrayOutputStream outputStream) throws IOException {
    writeBytes(Snappy.compress(primitives), outputStream);
  }
  private void writePrimitiveInts(int[] primitives, ByteArrayOutputStream outputStream) throws IOException {
    writeBytes(Snappy.compress(primitives), outputStream);
  }

  private void writeBytes(byte[] bytes, ByteArrayOutputStream outputStream) throws IOException {
    outputStream.write(bytes.length);
    outputStream.write(bytes);
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
      int columnType = input.read();
      
      int compressedNullsSize = input.read();
      byte[] compressedNulls = new byte[compressedNullsSize];
      input.read(compressedNulls, 0, compressedNullsSize);

      int compressedValsSize = input.read();
      byte[] compressedVals = new byte[compressedValsSize];
      input.read(compressedVals, 0, compressedValsSize);
      
      try {
        switch (TTypeId.findByValue(columnType)) {
        case BOOLEAN_TYPE:
          TBoolColumn column = new TBoolColumn(TColumn._Fields.findByThriftId(columnType), (Object) Snappy.uncompress(compressedCol));
          break;
        case TINYINT_TYPE:
          break;
        case SMALLINT_TYPE:
          break;
        case INT_TYPE:
          break;
        case BIGINT_TYPE:
          break;
        case DOUBLE_TYPE:
          break;
        case BINARY_TYPE:
          break;
        case STRING_TYPE:
          break;
        default:
          throw new IllegalStateException("Unrecognized column type");
        }
        outputCols[colNum] = new ColumnBuffer(column);
      } catch (IOException e) {
        e.printStackTrace();
      }
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
