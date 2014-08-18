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
package org.apache.sentry.hdfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

public class AuthzUpdate implements UpdateForwarder.Update {
  
  public static class PrivilegeUpdate {
    private final String authzObj;
    private final Map<String, String> addPrivileges = new HashMap<String, String>();
    private final Map<String, String> delPrivileges = new HashMap<String, String>();
    public PrivilegeUpdate(String authzObj) {
      this.authzObj = authzObj;
    }
    public PrivilegeUpdate addPrivilege(String role, String action) {
      addPrivileges.put(role, action);
      return this;
    }
    public PrivilegeUpdate delPrivilege(String role, String action) {
      delPrivileges.put(role, action);
      return this;
    }
    public String getAuthzObj() {
      return authzObj;
    }
    public Map<String, String> getAddPrivileges() {
      return addPrivileges;
    }
    public Map<String, String> getDelPrivileges() {
      return delPrivileges;
    }
    
  }

  public static class RoleUpdate {
    private final String group;
    private final List<String> addRoles = new LinkedList<String>();
    private final List<String> delRoles = new LinkedList<String>();
    public RoleUpdate(String group) {
      this.group = group;
    }
    public RoleUpdate addRole(String role) {
      addRoles.add(role);
      return this;
    }
    public RoleUpdate delRole(String role) {
      delRoles.add(role);
      return this;
    }
    public String getGroup() {
      return group;
    }
    public List<String> getAddRoles() {
      return addRoles;
    }
    public List<String> getDelRoles() {
      return delRoles;
    }
    
  }

  public static String ALL_PRIVS = "__ALL_PRIVS__";
  public static String ALL_ROLES = "__ALL_ROLES__";

  private final int seqNum;
  private final boolean hasFullImage;
  private final List<RoleUpdate> roleUpdates = new LinkedList<RoleUpdate>();
  private final List<PrivilegeUpdate> privilegeUpdates = new LinkedList<PrivilegeUpdate>();
  public AuthzUpdate(int seqNum, boolean hasFullImage) {
    this.seqNum = seqNum;
    this.hasFullImage = hasFullImage;
  }

  @Override
  public int getSeqNum() {
    return seqNum;
  }

  @Override
  public boolean hasFullImage() {
    return hasFullImage;
  }

  public PrivilegeUpdate addPrivilegeUpdate(String authzObj) {
    if (!hasFullImage) {
      PrivilegeUpdate privUpdate = new PrivilegeUpdate(authzObj);
      privilegeUpdates.add(privUpdate);
      return privUpdate;
    }
    return null;
  }
  public RoleUpdate addRoleUpdate(String group) {
    if (!hasFullImage) {
      RoleUpdate roleUpdate = new RoleUpdate(group);
      roleUpdates.add(roleUpdate);
      return roleUpdate;
    }
    return null;
  }
  public List<RoleUpdate> getRoleUpdates() {
    return roleUpdates;
  }
  public List<PrivilegeUpdate> getPrivilegeUpdates() {
    return privilegeUpdates;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static String toJsonString(AuthzUpdate update) throws IOException {
    Map retMap = new HashMap();
    retMap.put("seqNum", update.seqNum);
    retMap.put("hasFullImage", update.hasFullImage);
    List jsonPrivilegeUpdates = new LinkedList();
    for (PrivilegeUpdate privilegeUpdate : update.privilegeUpdates) {
      Map ruMap = new HashMap();
      ruMap.put("authzObj", privilegeUpdate.authzObj);
      ruMap.put("addPrivileges", privilegeUpdate.getAddPrivileges());
      ruMap.put("delPrivileges", privilegeUpdate.getDelPrivileges());
      jsonPrivilegeUpdates.add(ruMap);
    }
    retMap.put("privilegeUpdates", jsonPrivilegeUpdates);
    List jsonRoleUpdates = new LinkedList();
    for (RoleUpdate roleUpdate : update.roleUpdates) {
      Map ruMap = new HashMap();
      ruMap.put("group", roleUpdate.group);
      ruMap.put("addRoles", roleUpdate.getAddRoles());
      ruMap.put("delRoles", roleUpdate.getDelRoles());
      jsonRoleUpdates.add(ruMap);
    }
    retMap.put("roleUpdates", jsonRoleUpdates);
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(retMap);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static AuthzUpdate fromJsonString(String jsonString) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    Map jsonMap = mapper.readValue(jsonString, Map.class);
    Integer seqNum = (Integer)jsonMap.get("seqNum");
    Boolean hasFullImage = (Boolean)jsonMap.get("hasFullImage");
    AuthzUpdate authzUpdate = new AuthzUpdate(seqNum, hasFullImage);
    for (Map puJson : (List<Map>)jsonMap.get("privilegeUpdates")) {
      String authzObj = (String)puJson.get("authzObj");
      Map<String, String> addPrivileges = (Map<String, String>)puJson.get("addPrivileges");
      Map<String, String> delPrivileges = (Map<String, String>)puJson.get("delPrivileges");
      PrivilegeUpdate pu = authzUpdate.addPrivilegeUpdate(authzObj);
      for (Map.Entry<String, String> ap : addPrivileges.entrySet()) {
        pu.addPrivilege(ap.getKey(), ap.getValue());
      }
      for (Map.Entry<String, String> dp : delPrivileges.entrySet()) {
        pu.delPrivilege(dp.getKey(), dp.getValue());
      }
    }
    for (Map ruJson : (List<Map>) jsonMap.get("roleUpdates")) {
      String group = (String)ruJson.get("group");
      List<String> addRoles = (List<String>)ruJson.get("addRoles");
      List<String> delRoles = (List<String>)ruJson.get("delRoles");
      RoleUpdate ru = authzUpdate.addRoleUpdate(group);
      for (String ar : addRoles) {
        ru.addRole(ar);
      }
      for (String dr : delRoles) {
        ru.delRole(dr);
      }
    }
    return authzUpdate;
  }
  
}
