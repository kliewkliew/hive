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

import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * Load classes that implement CompDe when starting up, and serve them at run
 * time.
 *
 */
public class CompDeServiceLoader {
  private static CompDeServiceLoader instance;

  // Map CompDe names to classes so we don't have to read the META-INF file for every session.
  private ImmutableMap<ImmutablePair<String,String>, Class<? extends CompDe>> compdeTable;

  private static final Logger LOG = LoggerFactory.getLogger(CompDeServiceLoader.class);

  /**
   * Get the singleton instance or initialize the CompDeServiceLoader.
   * @return A singleton instance of CompDeServiceLoader.
   */
  public static synchronized CompDeServiceLoader getInstance() {
    if (instance == null) {
      Iterator<CompDe> compdes = ServiceLoader.load(CompDe.class).iterator();
      instance = new CompDeServiceLoader();
      ImmutableMap.Builder<ImmutablePair<String,String>, Class<? extends CompDe>> compdeMapBuilder =
          new ImmutableMap.Builder<>();
      while (compdes.hasNext()) {
        CompDe compde = compdes.next();
        compdeMapBuilder.put(
            ImmutablePair.of(
                compde.getVendor() + "." + compde.getName(),
                compde.getVersion()),
            compde.getClass());
      }
      instance.compdeTable = compdeMapBuilder.build();
    }
    return instance;
  }

  /**
   * Get a new instance of the CompDe.
   * @param compdeName The compressor name qualified by the vendor namespace.
   * ie. hive.snappy
   * @param version The plug-in version.
   * @return A CompDe implementation object.
   * @throws Exception if the plug-in cannot be instantiated.
   */
  public CompDe getCompde(String compdeName, String version) throws Exception {
    try {
      ImmutablePair<String,String> requestedCompde =
          ImmutablePair.of(compdeName, version);
      CompDe compde = compdeTable.get(requestedCompde).newInstance();
      LOG.debug("Instantiated CompDe plugin for " + compdeName);
      return compde;
    } catch (Exception e) {
      LOG.debug("CompDe plug-in cannot be instantiated for " + compdeName);
      throw e;
    }
  }

}
