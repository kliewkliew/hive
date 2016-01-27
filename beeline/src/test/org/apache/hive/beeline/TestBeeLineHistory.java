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

package org.apache.hive.beeline;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestBeeLineHistory - executes tests of the !history command of BeeLine
 */
public class TestBeeLineHistory {

  private static final String fileName = "history";

  @BeforeClass
  public static void beforeTests() throws Exception {
    PrintWriter writer = new PrintWriter(fileName);
    writer.println("select 1;");
    writer.println("select 2;");
    writer.close();
  }

  @Test
  public void testNumHistories() throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ops = new PrintStream(os);
    BeeLine beeline = new BeeLine();
    beeline.getOpts().setHistoryFile(fileName);
    beeline.setOutputStream(ops);
    beeline.getConsoleReader(null);
    beeline.dispatch("!history");
    String output = os.toString("UTF-8");
    int numHistories = output.split("\n").length;
    Assert.assertEquals(numHistories, 2);
    beeline.close();
  }

  @AfterClass
  public static void afterTests() throws Exception {
    File file = new File(fileName);
    file.delete();
  }
}