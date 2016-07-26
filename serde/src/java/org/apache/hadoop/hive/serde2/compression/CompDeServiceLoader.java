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

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load classes that implement CompDe when starting up, and serve them at run
 * time.
 *
 */
public class CompDeServiceLoader {
  private static CompDeServiceLoader instance;
  // Map CompDe names to classes so we don't have to read the META-INF file for every session.
  private ConcurrentHashMap<String, Class<? extends CompDe>> compressorTable
    = new ConcurrentHashMap<String, Class<? extends CompDe>>();
  public static final Logger LOG = LoggerFactory.getLogger(CompDeServiceLoader.class);


  /**
   * Get the singleton instance or initialize the CompDeServiceLoader.
   *
   * @return A singleton instance of CompDeServiceLoader.
   */
  public static synchronized CompDeServiceLoader getInstance() {
    if (instance == null) {
      Iterator<CompDe> compressors = ServiceLoader.load(CompDe.class).iterator();
      instance = new CompDeServiceLoader();
      while (compressors.hasNext()) {
        CompDe compressor = compressors.next();
        instance.compressorTable.put(
            compressor.getVendor() + "." + compressor.getName(),
            compressor.getClass());
      }
    }
    return instance;
  }

  /**
   * Get the CompDe if the compressor class was loaded from CLASSPATH.
   * 
   * @param compDeName
   *          The compressor name qualified by the vendor namespace.
   *          
   * @return A CompDe implementation object
   */
  public CompDe getCompDe(String compDeName) {
    try {
      return compressorTable.get(compDeName).newInstance();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

}
