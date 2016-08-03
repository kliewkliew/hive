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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

public class TestCompDeNegotiation {
  private HiveConf noCompDes;
  private HiveConf singleCompDe;
  private HiveConf multiCompDes1;
  private HiveConf multiCompDes2;

  @Before
  public void init() throws Exception {
    noCompDes = new HiveConf();
    noCompDes.setBoolVar(ConfVars.COMPRESSRESULT, true);
    noCompDes.setBoolean("datanucleus.schema.autoCreateTables", true);

    noCompDes.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, HiveAuthFactory.AuthTypes.NONE.toString());
    noCompDes.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider");
    //noCompDes.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER, SessionStateUserAuthenticator.class.getName());
    //noCompDes.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    
    singleCompDe = new HiveConf();
    singleCompDe.setBoolVar(ConfVars.COMPRESSRESULT, true);
    singleCompDe.setVar(ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_SERVER_COMPRESSORS, "compde3");
    singleCompDe.setBoolean("datanucleus.schema.autoCreateTables", true);

    singleCompDe.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, HiveAuthFactory.AuthTypes.NONE.toString());
    singleCompDe.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider");
    //noCompDes.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER, SessionStateUserAuthenticator.class.getName());
    //noCompDes.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);

    multiCompDes1 = new HiveConf();
    multiCompDes1.setBoolVar(ConfVars.COMPRESSRESULT, true);
    multiCompDes1.setVar(ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_SERVER_COMPRESSORS, "compde1,compde2,compde3,compde4");
    multiCompDes1.setBoolean("datanucleus.schema.autoCreateTables", true);

    multiCompDes1.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, HiveAuthFactory.AuthTypes.NONE.toString());
    multiCompDes1.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider");
    //noCompDes.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER, SessionStateUserAuthenticator.class.getName());
    //noCompDes.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);

    multiCompDes2 = new HiveConf();
    multiCompDes2.setBoolVar(ConfVars.COMPRESSRESULT, true);
    multiCompDes2.setVar(ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_SERVER_COMPRESSORS, "compde2, compde4");
    multiCompDes2.setBoolean("datanucleus.schema.autoCreateTables", true);
  }

  @Test
  public void testServerWithoutCompDe() throws HiveSQLException, InterruptedException, TException {
    ThriftCLIService service = new EmbeddedThriftBinaryCLIService();
    service.init(noCompDes);

    TOpenSessionReq req = new TOpenSessionReq();
    req.setClient_protocol(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9);
    req.setUsername("username");
    req.setPassword("password");
    req.setConfiguration(new HashMap<String, String>());
    TOpenSessionResp resp;

    req.setConfiguration(noCompDes.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals(null, resp.getCompressorName());

    req.setConfiguration(singleCompDe.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals(null, resp.getCompressorName());

    req.setConfiguration(multiCompDes2.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals(null, resp.getCompressorName());

    service.stop();
  }

  @Test
  public void testServerSingleCompDe() throws HiveSQLException, InterruptedException, TException {
    ThriftCLIService service = new EmbeddedThriftBinaryCLIService();
    service.init(singleCompDe);

    TOpenSessionReq req = new TOpenSessionReq();
    req.setClient_protocol(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9);
    req.setUsername("username");
    req.setPassword("password");
    req.setConfiguration(new HashMap<String, String>());
    TOpenSessionResp resp;

    req.setConfiguration(noCompDes.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals(null, resp.getCompressorName());

    req.setConfiguration(singleCompDe.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals("compde3", resp.getCompressorName());

    req.setConfiguration(multiCompDes2.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals("compde2", resp.getCompressorName());

    service.stop();
  }

  // @Test
  public void testServerWithMultiCompDes() throws HiveSQLException, InterruptedException, TException {
    ThriftCLIService service = new EmbeddedThriftBinaryCLIService();
    service.init(multiCompDes1);

    TOpenSessionReq req = new TOpenSessionReq();
    req.setClient_protocol(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9);
    req.setUsername("username");
    req.setPassword("password");
    req.setConfiguration(new HashMap<String, String>());
    TOpenSessionResp resp;

    req.setConfiguration(noCompDes.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals(null, resp.getCompressorName());

    req.setConfiguration(singleCompDe.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals("compde3", resp.getCompressorName());

    req.setConfiguration(multiCompDes1.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals("compde1", resp.getCompressorName());

    req.setConfiguration(multiCompDes2.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals("compde2", resp.getCompressorName());

    service.stop();
  }

}
