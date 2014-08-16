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
package org.apache.sentry.provider.db.service.cache;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

public class HMSUpdate {
  
  public static String ALL_PATHS = "__ALL_PATHS__";

  public static class PathUpdate {
    private final String authzObj;
    private final List<String> addPaths = new LinkedList<String>();
    private final List<String> delPaths = new LinkedList<String>();
    private PathUpdate(String authzObj) {
      this.authzObj = authzObj;
    }
    public PathUpdate addPath(String path) {
      addPaths.add(path);
      return this;
    }
    public PathUpdate delPath(String path) {
      delPaths.add(path);
      return this;
    }
    public String getAuthzObj() {
      return authzObj;
    }
    public List<String> getAddPaths() {
      return addPaths;
    }
    public List<String> getDelPaths() {
      return delPaths;
    }
    
  }

  private final int seqNum;
  private final boolean hasFullImage;
  private final String pathDump;
  private final List<PathUpdate> pathUpdates = new LinkedList<PathUpdate>();
  public HMSUpdate(int seqNum, String pathDump) {
    this.seqNum = seqNum;
    this.pathDump = pathDump;
    this.hasFullImage = (pathDump != null);
  }
  public boolean hasFullImage() {
    return hasFullImage;
  }
  public PathUpdate addPathUpdate(String authzObject) {
    if (!hasFullImage) {
      PathUpdate pathUpdate = new PathUpdate(authzObject);
      pathUpdates.add(pathUpdate);
      return pathUpdate;
    }
    return null;
  }
  public List<PathUpdate> getPathUpdates() {
    return pathUpdates;
  }
  public int getSeqNum() {
    return seqNum;
  }
  public String getPathDump() {
    return pathDump;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static String toJsonString(HMSUpdate update) throws IOException {
    Map retMap = new HashMap();
    retMap.put("seqNum", update.seqNum);
    retMap.put("pathDump", update.pathDump);
    List jsonPathUpdates = new LinkedList();
    for (PathUpdate pathUpdate : update.pathUpdates) {
      Map puMap = new HashMap();
      puMap.put("authzObj", pathUpdate.authzObj);
      puMap.put("addPaths", pathUpdate.getAddPaths());
      puMap.put("delPaths", pathUpdate.getDelPaths());
      jsonPathUpdates.add(puMap);
    }
    retMap.put("pathUpdates", jsonPathUpdates);
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(retMap);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static HMSUpdate fromJsonString(String jsonString) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    Map jsonMap = mapper.readValue(jsonString, Map.class);
    Integer seqNum = (Integer)jsonMap.get("seqNum");
    String pathDump = (String)jsonMap.get("pathDump");
    HMSUpdate hmsUpdate = new HMSUpdate(seqNum, pathDump);
    for (Map puJson : (List<Map>)jsonMap.get("pathUpdates")) {
      String authzObj = (String)puJson.get("authzObj");
      List<String> addPaths = (List<String>)puJson.get("addPaths");
      List<String> delPaths = (List<String>)puJson.get("delPaths");
      PathUpdate pu = hmsUpdate.addPathUpdate(authzObj);
      for (String ap : addPaths) {
        pu.addPath(ap);
      }
      for (String dp : delPaths) {
        pu.delPath(dp);
      }
    }
    return hmsUpdate;
  }

}
