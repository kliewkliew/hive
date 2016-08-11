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

package org.apache.hive.service.cli.compression;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

public class TestCompDeNegotiation {
  private HiveConf noCompDes;
  private HiveConf singleCompDe;
  private HiveConf multiCompDes1;
  private HiveConf multiCompDes2;
  private HiveConf serverCompDeConf;
  private HiveConf clientCompDeConf;

  @Before
  public void init() throws Exception {
    HiveConf baseConf = new HiveConf();
    baseConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, HiveAuthFactory.AuthTypes.NONE.toString());
    baseConf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider");
    baseConf.setBoolean("datanucleus.schema.autoCreateTables", true);

    noCompDes = new HiveConf(baseConf);
    
    singleCompDe = new HiveConf(baseConf);
    singleCompDe.set(compressorListVarName(), "compde3");

    multiCompDes1 = new HiveConf(baseConf);
    multiCompDes1.set(compressorListVarName(), "compde1,compde2,compde3,compde4");

    multiCompDes2 = new HiveConf(baseConf);
    multiCompDes2.set(compressorListVarName(), "compde2, compde4");

    serverCompDeConf = new HiveConf(baseConf);
    serverCompDeConf.set(compressorListVarName(), "compde3");
    serverCompDeConf.set(compDeConfigPrefix("compde3") + ".test1", "serverVal1");
    serverCompDeConf.set(compDeConfigPrefix("compde3") + ".test2", "serverVal2");//overriden by client
    serverCompDeConf.set(compDeConfigPrefix("compde3") + ".test4", "serverVal4");//overriden by plug-in
    
    clientCompDeConf = new HiveConf(baseConf);
    serverCompDeConf.set(compressorListVarName(), "compde3");
    clientCompDeConf.set(compDeConfigPrefix("compde3") + ".test2", "clientVal2");//overrides server
    clientCompDeConf.set(compDeConfigPrefix("compde3") + ".test3", "clientVal3");
    clientCompDeConf.set(compDeConfigPrefix("compde3") + ".test5", "clientVal5");//overriden by plug-in
  }

  // The JDBC driver prefixes all configuration names and the server expects these prefixes
  private String compressorListVarName() {
    return "set:hiveconf:" + ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_COMPRESSOR_LIST.varname;
  }
  private String compDeConfigPrefix(String compDeName) {
    return "set:hiveconf:" + ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_COMPRESSOR.varname + "." + compDeName;
  }

  public class MockServiceWithoutCompDes extends EmbeddedThriftBinaryCLIService {
    @Override
    // Pretend that we have no CompDe plug-ins
    protected Map<String, String> initCompDe(String compDeName, Map<String, String> compDeConfig) {
      return null;
    }
  }

  @Test
  // The server has no CompDe plug-ins
  public void testServerWithoutCompDePlugins() throws HiveSQLException, InterruptedException, TException {
    ThriftCLIService service = new MockServiceWithoutCompDes();
    service.init(noCompDes);

    TOpenSessionReq req = new TOpenSessionReq();
    req.setConfiguration(new HashMap<String, String>());
    TOpenSessionResp resp;

    req.setConfiguration(noCompDes.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());

    req.setConfiguration(singleCompDe.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());

    req.setConfiguration(multiCompDes2.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());

    service.stop();
  }

  public class MockServiceWithCompDes extends EmbeddedThriftBinaryCLIService {
    @Override
    // Pretend that we have plug-ins for all CompDes except "compde1"
    protected Map<String, String> initCompDe(String compDeName, Map<String, String> compDeConfig) {
      if (compDeName.equals("compde1")) {
        return null;
      }
      else {
        return compDeConfig;
      }
    }
  }

  @Test
  // The server has plug-ins but the CompDe list is not configured
  public void testServerWithoutCompDeInList() throws HiveSQLException, InterruptedException, TException {
    ThriftCLIService service = new MockServiceWithCompDes();
    service.init(noCompDes);

    TOpenSessionReq req = new TOpenSessionReq();
    req.setConfiguration(new HashMap<String, String>());
    TOpenSessionResp resp;

    req.setConfiguration(noCompDes.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());

    req.setConfiguration(singleCompDe.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());

    req.setConfiguration(multiCompDes2.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());

    service.stop();
  }

  @Test
  public void testServerWithSingleCompDeInList() throws HiveSQLException, InterruptedException, TException {
    ThriftCLIService service = new MockServiceWithCompDes();
    service.init(singleCompDe);

    TOpenSessionReq req = new TOpenSessionReq();
    req.setConfiguration(new HashMap<String, String>());
    TOpenSessionResp resp;

    req.setConfiguration(noCompDes.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());

    req.setConfiguration(singleCompDe.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals("compde3", resp.getCompressorName());

    req.setConfiguration(multiCompDes2.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());

    service.stop();
  }

  @Test
  public void testServerWithMultiCompDesInList() throws HiveSQLException, InterruptedException, TException {
    ThriftCLIService service = new MockServiceWithCompDes();
    service.init(multiCompDes1);

    TOpenSessionReq req = new TOpenSessionReq();
    req.setConfiguration(new HashMap<String, String>());
    TOpenSessionResp resp;

    req.setConfiguration(noCompDes.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());

    req.setConfiguration(singleCompDe.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals("compde3", resp.getCompressorName());

    req.setConfiguration(multiCompDes1.getValByRegex(".*"));
    resp = service.OpenSession(req);
    // "compde1" fails to initialize because our mock service does not have that plugin
    assertEquals("compde2", resp.getCompressorName());

    req.setConfiguration(multiCompDes2.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals("compde2", resp.getCompressorName());

    service.stop();
  }

  public class MockWithCompDeConfig extends EmbeddedThriftBinaryCLIService {
    @Override
    // Mock a plug-in with an `init` function.
    protected Map<String, String> initCompDe(String compDeName, Map<String, String> compDeConfig) {
      compDeConfig.put(compDeConfigPrefix("compde3") + ".test4", "compDeVal4");//overrides server
      compDeConfig.put(compDeConfigPrefix("compde3") + ".test5", "compDeVal5");//overrides client
      compDeConfig.put(compDeConfigPrefix("compde3") + ".test6", "compDeVal6");
      return compDeConfig;
    }
  }

  @Test
  // Ensure that the server combines the server default CompDe configuration with the client overrides and lets the plug-in `init` function create the final configuration.
  public void testConfig() throws TException {
    Map<String, String> expectedConf = new HashMap<String, String>();
    expectedConf.put(compDeConfigPrefix("compde3") + ".test1", "serverVal1");
    expectedConf.put(compDeConfigPrefix("compde3") + ".test2", "clientVal2");
    expectedConf.put(compDeConfigPrefix("compde3") + ".test3", "clientVal3");
    expectedConf.put(compDeConfigPrefix("compde3") + ".test4", "compDeVal4");
    expectedConf.put(compDeConfigPrefix("compde3") + ".test5", "compDeVal5");
    expectedConf.put(compDeConfigPrefix("compde3") + ".test6", "compDeVal6");

    ThriftCLIService service = new MockWithCompDeConfig();
    service.init(serverCompDeConf);

    TOpenSessionReq req = new TOpenSessionReq();
    req.setConfiguration(clientCompDeConf.getValByRegex(".*"));

    TOpenSessionResp resp = service.OpenSession(req);
    assertEquals(expectedConf, resp.getCompressorConfiguration());
  }
}
