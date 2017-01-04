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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde2.compression.SnappyCompDe;
import org.apache.hadoop.hive.serde2.thrift.ColumnBuffer;
import org.apache.hive.service.rpc.thrift.*;

import static org.junit.Assert.assertArrayEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

public class TestSnappyCompDe {
  private SnappyCompDe compDe = new SnappyCompDe();
  byte[] noNullMask = {0};
  byte[] firstNullMask = {1};
  byte[] secondNullMask = {2};
  byte[] thirdNullMask = {3};
  ColumnBuffer columnBinary;
  ColumnBuffer columnBool;
  ColumnBuffer columnByte;
  ColumnBuffer columnShort;
  ColumnBuffer columnInt;
  ColumnBuffer columnLong;
  ColumnBuffer columnDouble;
  ColumnBuffer columnStr;

  @Before
  public void init() throws Exception {
    ByteBuffer firstRow = ByteBuffer.wrap(new byte[]{2, 33, 7, 75, 5});
    ByteBuffer secondRow = ByteBuffer.wrap(new byte[]{3, 21, 6});
    ByteBuffer thirdRow = ByteBuffer.wrap(new byte[]{52, 25, 74, 74, 64});
    firstRow.flip();
    secondRow.flip();
    thirdRow.flip();
    ArrayList<ByteBuffer> someBinaries = new ArrayList<ByteBuffer>();
    someBinaries.add(firstRow);
    someBinaries.add(secondRow);
    someBinaries.add(thirdRow);
    columnBinary = new ColumnBuffer(TColumn.binaryVal(
        new TBinaryColumn(someBinaries, ByteBuffer.wrap(new byte[]{}))));

    // Test leading and trailing `false` in column
    ArrayList<Boolean> bools = new ArrayList<Boolean>();
    bools.add(false);
    bools.add(true);
    bools.add(false);
    bools.add(true);
    bools.add(false);
    columnBool = new ColumnBuffer(TColumn.boolVal(
        new TBoolColumn(bools, ByteBuffer.wrap(noNullMask))));

    ArrayList<Byte> bytes = new ArrayList<Byte>();
    bytes.add((byte) 0);
    bytes.add((byte) 1);
    bytes.add((byte) 2);
    bytes.add((byte) 3);
    columnByte= new ColumnBuffer(TColumn.byteVal(
        new TByteColumn(bytes, ByteBuffer.wrap(noNullMask))));

    ArrayList<Short> shorts = new ArrayList<Short>();
    shorts.add((short) 0);
    shorts.add((short) 1);
    shorts.add((short) -127);
    shorts.add((short) 127);
    columnShort= new ColumnBuffer(TColumn.i16Val(
        new TI16Column(shorts, ByteBuffer.wrap(noNullMask))));

    ArrayList<Integer> ints = new ArrayList<Integer>();
    ints.add(0);
    ints.add(1);
    ints.add(-32767);
    ints.add(32767);
    columnInt= new ColumnBuffer(TColumn.i32Val(
        new TI32Column(ints, ByteBuffer.wrap(noNullMask))));

    ArrayList<Long> longs = new ArrayList<Long>();
    longs.add((long) 0);
    longs.add((long) 1);
    longs.add((long) -2147483647 );
    longs.add((long) 2147483647 );
    columnLong = new ColumnBuffer(TColumn.i64Val(
        new TI64Column(longs, ByteBuffer.wrap(noNullMask))));

    ArrayList<Double> doubles = new ArrayList<Double>();
    doubles.add((double) 0);
    doubles.add((double) 1.0);
    doubles.add((double) -2147483647.5 );
    doubles.add((double) 2147483647.5 );
    columnDouble= new ColumnBuffer(TColumn.doubleVal(
        new TDoubleColumn(doubles, ByteBuffer.wrap(noNullMask))));

    ArrayList<String> strings = new ArrayList<String>();
    strings.add("ABC");
    strings.add("DEFG");
    strings.add("HI");
    strings.add(StringUtils.rightPad("", 65535, 'j'));
    strings.add("");
    columnStr = new ColumnBuffer(TColumn.stringVal(
        new TStringColumn(strings, ByteBuffer.wrap(noNullMask))));

    compDe.init(new HashMap<String, String>());
  }

  @Test
  public void testBinaryCol() throws Exception {
    ColumnBuffer[] inputCols = new ColumnBuffer[]{columnBinary};

    ByteBuffer compressed = compDe.compress(inputCols);
    ColumnBuffer[] outputCols = compDe.decompress(compressed, compressed.limit());

    assertArrayEquals(
        inputCols[0].toTColumn().getBinaryVal().getValues().toArray(),
        outputCols[0].toTColumn().getBinaryVal().getValues().toArray());
  }

  @Test
  public void testBoolCol() throws Exception {
    ColumnBuffer[] inputCols = new ColumnBuffer[]{columnBool};

    ByteBuffer compressed = compDe.compress(inputCols);
    ColumnBuffer[] outputCols = compDe.decompress(compressed, compressed.limit());

    assertArrayEquals(
        inputCols[0].toTColumn().getBoolVal().getValues().toArray(),
        outputCols[0].toTColumn().getBoolVal().getValues().toArray());
  }

  @Test
  public void testByteCol() throws Exception {
    ColumnBuffer[] inputCols = new ColumnBuffer[]{columnByte};

    ByteBuffer compressed = compDe.compress(inputCols);
    ColumnBuffer[] outputCols = compDe.decompress(compressed, compressed.limit());

    assertArrayEquals(
        inputCols[0].toTColumn().getByteVal().getValues().toArray(),
        outputCols[0].toTColumn().getByteVal().getValues().toArray());
  }

  @Test
  public void testIntCol() throws Exception {
    ColumnBuffer[] inputCols = new ColumnBuffer[]{columnInt};

    ByteBuffer compressed = compDe.compress(inputCols);
    ColumnBuffer[] outputCols = compDe.decompress(compressed, compressed.limit());

    assertArrayEquals(
        inputCols[0].toTColumn().getI32Val().getValues().toArray(),
        outputCols[0].toTColumn().getI32Val().getValues().toArray());
  }

  @Test
  public void testLongCol() throws Exception {
    ColumnBuffer[] inputCols = new ColumnBuffer[]{columnLong};

    ByteBuffer compressed = compDe.compress(inputCols);
    ColumnBuffer[] outputCols = compDe.decompress(compressed, compressed.limit());

    assertArrayEquals(
        inputCols[0].toTColumn().getI64Val().getValues().toArray(),
        outputCols[0].toTColumn().getI64Val().getValues().toArray());
  }

  @Test
  public void testDoubleCol() throws Exception {
    ColumnBuffer[] inputCols = new ColumnBuffer[]{columnDouble};

    ByteBuffer compressed = compDe.compress(inputCols);
    ColumnBuffer[] outputCols = compDe.decompress(compressed, compressed.limit());

    assertArrayEquals(
        inputCols[0].toTColumn().getDoubleVal().getValues().toArray(),
        outputCols[0].toTColumn().getDoubleVal().getValues().toArray());
  }

  @Test
  public void testStringCol() throws Exception {
    ColumnBuffer[] inputCols = new ColumnBuffer[]{columnStr};

    ByteBuffer compressed = compDe.compress(inputCols);
    ColumnBuffer[] outputCols = compDe.decompress(compressed, compressed.limit());

    assertArrayEquals(
        inputCols[0].toTColumn().getStringVal().getValues().toArray(),
        outputCols[0].toTColumn().getStringVal().getValues().toArray());
  }

  @Test
  public void testNulls() throws Exception {
    ColumnBuffer[] inputCols;
    ArrayList<String> someStrings = new ArrayList<String>();
    someStrings.add("test1");
    someStrings.add("test2");
    ColumnBuffer columnStr1 = new ColumnBuffer(TColumn.stringVal(
        new TStringColumn(someStrings, ByteBuffer.wrap(firstNullMask))));
    ColumnBuffer columnStr2 = new ColumnBuffer(TColumn.stringVal(
        new TStringColumn(someStrings, ByteBuffer.wrap(secondNullMask))));
    ColumnBuffer columnStr3 = new ColumnBuffer(TColumn.stringVal(
        new TStringColumn(someStrings, ByteBuffer.wrap(thirdNullMask))));

    inputCols = new ColumnBuffer[]{
        columnStr1,
        columnStr2,
        columnStr3};

    ByteBuffer compressed = compDe.compress(inputCols);
    ColumnBuffer[] outputCols = compDe.decompress(compressed, compressed.limit());

    assertArrayEquals(inputCols, outputCols);
  }

  @Test
  public void testMulti() throws Exception {
    ColumnBuffer[] inputCols = new ColumnBuffer[]{columnInt, columnStr};

    ByteBuffer compressed = compDe.compress(inputCols);
    ColumnBuffer[] outputCols = compDe.decompress(compressed, compressed.limit());

    assertArrayEquals(
        inputCols[0].toTColumn().getI32Val().getValues().toArray(),
        outputCols[0].toTColumn().getI32Val().getValues().toArray());
    assertArrayEquals(
        inputCols[1].toTColumn().getStringVal().getValues().toArray(),
        outputCols[1].toTColumn().getStringVal().getValues().toArray());
  }
}
