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
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.Service;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.hive.service.cli.thrift.ThriftHttpCLIService;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.server.HiveServer2;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

public class TestCompDeNegotiation {
  private HiveConf noCompDes;
  private HiveConf singleCompDe;
  private HiveConf multiCompDes1;
  private HiveConf multiCompDes2;
  protected static int port;

  @Before
  public void init() throws InterruptedException, HiveSQLException, IOException {
    noCompDes = new HiveConf();
    noCompDes.setBoolVar(ConfVars.COMPRESSRESULT, true);

    singleCompDe = new HiveConf();
    singleCompDe.setVar(ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_SERVER_COMPRESSORS, "compde3");

    multiCompDes1 = new HiveConf();
    multiCompDes1.setVar(ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_SERVER_COMPRESSORS, "compde1,compde2,compde3,compde4");

    multiCompDes2 = new HiveConf();
    multiCompDes2.setVar(ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_SERVER_COMPRESSORS, "compde2, compde4");



  }

  private ThriftCLIService startCLIService(HiveServer2 server, HiveConf conf) throws InterruptedException {
    // Start hive server2
    CLIService cliService = new CLIService(server);
    cliService.init(conf);
    ThriftCLIService service = new ThriftBinaryCLIService(cliService, null);
    service.init(conf);
    service.start();
    Thread.sleep(5000);
    System.out.println("## HiveServer started");
    return service;
  }

  private void stopHiveServer(HiveServer2 server) {
    if (server != null) {
      // kill server
      server.stop();
    }
  }

  protected SessionHandle openSession(HiveServer2 server, HiveConf conf) throws HiveSQLException {
    for (Service service : server.getServices()) {
      if (service instanceof ThriftBinaryCLIService) {
        ThriftCLIServiceClient client = new ThriftCLIServiceClient((ThriftBinaryCLIService) service);
        return client.openSession("user", "password");
      }
      if (service instanceof ThriftHttpCLIService) {
        ThriftCLIServiceClient client =  new ThriftCLIServiceClient((ThriftHttpCLIService) service);
        return client.openSession("user", "password");
      }
    }
    throw new IllegalStateException("HiveServer2 not running Thrift service");
  }
  
  private ThriftBinaryCLIService getService(HiveServer2 server) {
    for (Service service : server.getServices()) {
      if (service instanceof ThriftBinaryCLIService) {
        return (ThriftBinaryCLIService) service;
      }
    }
    throw new IllegalStateException("HiveServer2 not running Thrift service");
  }


  @Test
  public void testServerWithoutCompDe() throws HiveSQLException, InterruptedException, TException {
    HiveServer2 server = new HiveServer2();
    ThriftCLIService service = startCLIService(server, singleCompDe);
    //ThriftBinaryCLIService service = getService(server);

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
    stopHiveServer(server);
  }

  @Test
  public void testServerSingleCompDe() throws HiveSQLException, InterruptedException, TException {
    HiveServer2 server = new HiveServer2();
    ThriftCLIService service = startCLIService(server, singleCompDe);

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
    assertEquals("1", resp.getSessionHandle());
    assertEquals("compde3", resp.getCompressorName());

    req.setConfiguration(multiCompDes2.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals("compde2", resp.getCompressorName());

    service.stop();
    stopHiveServer(server);
  }

  //@Test
  public void testServerWithMultiCompDes() throws HiveSQLException, InterruptedException, TException {
    HiveServer2 server = new HiveServer2();
    ThriftCLIService service = startCLIService(server, multiCompDes1);

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
    stopHiveServer(server);
  }

}
