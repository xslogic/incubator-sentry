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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.sentry.hdfs.AuthzUpdate.PrivilegeUpdate;
import org.apache.sentry.hdfs.AuthzUpdate.RoleUpdate;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class AuthzCache extends UpdateForwarder<AuthzUpdate>{

  static class PrivilegeInfo {
    private final String authzObj;
    private final Map<String, FsAction> roleToPermission = new HashMap<String, FsAction>();
    PrivilegeInfo(String authzObj) {
      this.authzObj = authzObj;
    }
    PrivilegeInfo setPermission(String role, FsAction perm) {
      roleToPermission.put(role, perm);
      return this;
    }
    PrivilegeInfo removePermission(String role) {
      roleToPermission.remove(role);
      return this;
    }
    FsAction getPermission(String role) {
      return roleToPermission.get(role);
    }
  }

  static class RoleInfo {
    private final String group;
    private final Set<String> roles = new HashSet<String>();
    public RoleInfo(String group) {
      this.group = group;
    }
    RoleInfo addRole(String role) {
      roles.add(group);
      return this;
    }
    RoleInfo delRole(String role) {
      roles.remove(group);
      return this;
    }
  }

  public interface AuthzSource {
    public PrivilegeInfo loadPrivilege(String authzObj) throws Exception;
    public RoleInfo loadRole(String role) throws Exception;
    public AuthzUpdate createFullImage(int seqNum);
  }

  private final LoadingCache<String, PrivilegeInfo> privilegeCache;
  private final LoadingCache<String, RoleInfo> roleCache;
  private final AuthzSource source;
  
  public AuthzCache(long cacheExpiry, final AuthzSource source,
      int updateLogSize) {
    super(updateLogSize);
    this.source = source;
    privilegeCache = CacheBuilder.newBuilder()
        .expireAfterAccess(cacheExpiry, TimeUnit.MILLISECONDS)
        .build(new CacheLoader<String, PrivilegeInfo>() {
          @Override
          public PrivilegeInfo load(String key) throws Exception {
            return source.loadPrivilege(key);
          }
        });
    roleCache = CacheBuilder.newBuilder()
        .expireAfterAccess(cacheExpiry, TimeUnit.MILLISECONDS)
        .build(new CacheLoader<String, RoleInfo>() {
          @Override
          public RoleInfo load(String key) throws Exception {
            return source.loadRole(key);
          }
        });
  }

  public FsAction getPermission(String authzObj, String group) {
    RoleInfo roleInfo = roleCache.getUnchecked(group);
    if (roleInfo != null) {
      PrivilegeInfo privilegeInfo = privilegeCache.getUnchecked(authzObj);
      FsAction retVal = FsAction.NONE;
      if (privilegeInfo != null) {
        for (String role : roleInfo.roles) {
          FsAction perm = privilegeInfo.getPermission(role);
          if (perm != null) {
            retVal = retVal.or(perm);
          }
        }
        return retVal;
      }
    }
    return null;
  }

  

  @Override
  protected AuthzUpdate retrieveFullImageFromSourceAndApply(int currSeqNum) {
    AuthzUpdate fullImage = source.createFullImage(currSeqNum);
    applyFullImageUpdate(fullImage);
    return fullImage;
  }

  @Override
  protected void applyFullImageUpdate(AuthzUpdate update) {
    if (update.hasFullImage()) {
      privilegeCache.invalidateAll();
      roleCache.invalidateAll();
    }
    applyPartialUpdate(update);
  }

  @Override
  protected AuthzUpdate createFullImageUpdate(int currSeqNum) {
    AuthzUpdate retVal = new AuthzUpdate(currSeqNum, true);
    for (Map.Entry<String, PrivilegeInfo> pE : privilegeCache.asMap()
        .entrySet()) {
      PrivilegeUpdate pUpdate = retVal.addPrivilegeUpdate(pE.getKey());
      PrivilegeInfo pInfo = pE.getValue();
      for (Map.Entry<String, FsAction> ent : pInfo.roleToPermission.entrySet()) {
        pUpdate.addPrivilege(ent.getKey(), ent.getValue().toString());
      }
    }
    for (Map.Entry<String, RoleInfo> rE : roleCache.asMap().entrySet()) {
      RoleUpdate rUpdate = retVal.addRoleUpdate(rE.getKey());
      RoleInfo rInfo = rE.getValue();
      for (String role : rInfo.roles) {
        rUpdate.addRole(role);
      }
    }
    return retVal;
  }

  @Override
  protected void applyPartialUpdate(AuthzUpdate update) {
    for (PrivilegeUpdate pUpdate : update.getPrivilegeUpdates()) {
      // Don't use the cache.get() method here.. don't want to
      // call the loader
      PrivilegeInfo pInfo = privilegeCache.getIfPresent(pUpdate.getAuthzObj());
      if (pInfo == null) {
        pInfo = new PrivilegeInfo(pUpdate.getAuthzObj());
        privilegeCache.put(pUpdate.getAuthzObj(), pInfo);
      }
      for (Map.Entry<String, String> aMap : pUpdate.getAddPrivileges().entrySet()) {
        FsAction fsAction = pInfo.getPermission(aMap.getKey());
        if (fsAction == null) {
          fsAction = FsAction.getFsAction(aMap.getValue());
        } else {
          fsAction = fsAction.or(FsAction.getFsAction(aMap.getValue()));
        }
        pInfo.setPermission(aMap.getKey(), fsAction);
      }
      for (Map.Entry<String, String> dMap : pUpdate.getAddPrivileges().entrySet()) {
        if (dMap.getKey().equals(AuthzUpdate.ALL_PRIVS)) {
          // Remove all privileges
          privilegeCache.invalidate(pUpdate.getAuthzObj());
          break;
        }
        FsAction fsAction = pInfo.getPermission(dMap.getKey());
        if (fsAction != null) {
          fsAction = fsAction.and(FsAction.getFsAction(dMap.getValue()).not());
          if (FsAction.NONE == fsAction) {
            pInfo.removePermission(dMap.getKey());
          } else {
            pInfo.setPermission(dMap.getKey(), fsAction);
          }
        }
      }
    }
    for(RoleUpdate rUpdate : update.getRoleUpdates()) {
      // Don't use the cache.get() method here.. don't want to
      // call the loader
      RoleInfo rInfo = roleCache.getIfPresent(rUpdate.getGroup());
      if (rInfo == null) {
        rInfo = new RoleInfo(rUpdate.getGroup());
        roleCache.put(rUpdate.getGroup(), rInfo);
      }
      for (String role : rUpdate.getAddRoles()) {
        rInfo.addRole(role);
      }
      for (String role : rUpdate.getDelRoles()) {
        if (role.equals(AuthzUpdate.ALL_ROLES)) {
          // Remove all roles
          roleCache.invalidate(rUpdate.getGroup());
          break;
        }
        rInfo.delRole(role);
      }
    }
  }
}
