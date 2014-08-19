package org.apache.sentry.hdfs;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.sentry.hdfs.AuthzCache.AuthzSource;
import org.apache.sentry.hdfs.AuthzCache.PrivilegeInfo;
import org.apache.sentry.hdfs.AuthzCache.RoleInfo;
import org.apache.sentry.hdfs.AuthzUpdate.PrivilegeUpdate;
import org.apache.sentry.hdfs.AuthzUpdate.RoleUpdate;

public class DummyAuthzSource implements AuthzSource{

  public Map<String, PrivilegeInfo> privs = new HashMap<String, PrivilegeInfo>();
  public Map<String, RoleInfo> roles = new HashMap<String, RoleInfo>();

  @Override
  public PrivilegeInfo loadPrivilege(String authzObj) throws Exception {
    return privs.get(authzObj);
  }

  @Override
  public RoleInfo loadRolesForGroup(String group) throws Exception {
    return roles.get(group);
  }

  @Override
  public AuthzUpdate createFullImage(long seqNum) {
    AuthzUpdate retVal = new AuthzUpdate(seqNum, true);
    for (Map.Entry<String, PrivilegeInfo> pE : privs.entrySet()) {
      PrivilegeUpdate pUpdate = retVal.addPrivilegeUpdate(pE.getKey());
      PrivilegeInfo pInfo = pE.getValue();
      for (Map.Entry<String, FsAction> ent : pInfo.roleToPermission.entrySet()) {
        pUpdate.addPrivilege(ent.getKey(), ent.getValue().SYMBOL);
      }
    }
    for (Map.Entry<String, RoleInfo> rE : roles.entrySet()) {
      RoleUpdate rUpdate = retVal.addRoleUpdate(rE.getKey());
      RoleInfo rInfo = rE.getValue();
      for (String role : rInfo.roles) {
        rUpdate.addRole(role);
      }
    }
    return retVal;
  }

}
