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
import java.util.Map;
import java.util.ServiceLoader;


/**
 * Load classes that implement CompDe when starting up, and serve
 * them at run time.
 *
 */
public class CompDeServiceLoader {
  private static CompDeServiceLoader instance;
  private static CompDe compDe;

  /**
   * Get the singleton instance of CompDeServiceLoader.
   *
   * @return A singleton instance of CompDeServiceLoader.
   */
  public static synchronized CompDeServiceLoader getInstance() {
    if (instance == null) {
      instance = new CompDeServiceLoader();
    }
    return instance;
  }
  
  /**
   * Initialize the CompDe
   * 
   * @param compDeName The compressor name qualified by the vendor namespace.
   * @param config
   * 
   * @return The final configuration returned by the CompDe upon successfuly initialization, else null.
   */
  public Map<String, String> initCompDe(String compDeName, Map<String, String> config) {
    Iterator<CompDe> compressors = ServiceLoader.load(CompDe.class).iterator();
    while (compressors.hasNext()) {
      CompDe compressor = compressors.next();
      if (compressor.getVendor() + "." + compressor.getName() == compDeName) {
        compDe = compressor;
        Map<String, String> compDeResponse = compDe.init(config);
        if (compDeResponse != null) {
          return compDe.init(config);
        }
      }
    }
    return null;
  }

  /**
   * Get the CompDe if the compressor class was loaded from CLASSPATH.
   * 
   * @return A CompDe implementation object
   */
  public CompDe getCompDe() {
    return compDe;
  }
  
}
