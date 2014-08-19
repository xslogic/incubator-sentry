package org.apache.sentry.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.sentry.hdfs.AuthzCache.PrivilegeInfo;
import org.apache.sentry.hdfs.AuthzCache.RoleInfo;
import org.junit.Test;

public class TestAuthzCache {

  @Test
  public void testAuthzAddRemove() throws InterruptedException {
    DummyAuthzSource src = new DummyAuthzSource();
    AuthzCache authzCache = new AuthzCache(10000, src, 0);
    src.privs.put("db1.tbl11", new PrivilegeInfo("db1.tbl11").setPermission("r1", FsAction.READ_WRITE));
    src.privs.put("db1.tbl12", new PrivilegeInfo("db1.tbl12").setPermission("r2", FsAction.READ));
    src.privs.put("db1.tbl13", new PrivilegeInfo("db1.tbl13").setPermission("r3", FsAction.WRITE));
    src.roles.put("g1", new RoleInfo("g1").addRole("r1"));
    src.roles.put("g2", new RoleInfo("g2").addRole("r2").addRole("r1"));
    src.roles.put("g3", new RoleInfo("g3").addRole("r3").addRole("r2").addRole("r1"));
    authzCache.handleUpdateNotification(new AuthzUpdate(10, false));
    waitToCommit(authzCache);
    
    assertEquals(FsAction.READ_WRITE, authzCache.getPermission("db1.tbl11", "g1"));
    assertEquals(FsAction.NONE, authzCache.getPermission("db1.tbl12", "g1"));
    assertEquals(FsAction.NONE, authzCache.getPermission("db1.tbl13", "g1"));

    assertEquals(FsAction.READ_WRITE, authzCache.getPermission("db1.tbl11", "g3"));
    assertEquals(FsAction.READ, authzCache.getPermission("db1.tbl12", "g3"));
    assertEquals(FsAction.WRITE, authzCache.getPermission("db1.tbl13", "g3"));
  }

  private void waitToCommit(AuthzCache authzCache) throws InterruptedException {
    int counter = 0;
    while(!authzCache.areAllUpdatesCommited()) {
      Thread.sleep(200);
      counter++;
      if (counter > 10000) {
        fail("Updates taking too long to commit !!");
      }
    }
  }
}
