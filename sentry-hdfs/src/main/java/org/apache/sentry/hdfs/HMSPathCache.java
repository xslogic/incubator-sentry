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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.sentry.hdfs.HMSUpdate.PathUpdate;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class HMSPathCache extends UpdateForwarder<HMSUpdate>{

  static class PathSeg {
    private final PathSeg parent;
    private final String segment;
    private final Map<String, PathSeg> children =
        new IdentityHashMap<String, PathSeg>();
    // Will be null of not associated to a db/table
    private String authzObj;
    private PathSeg(PathSeg parent, String segment) {
      this.parent = parent;
      this.segment = segment;
    }

    public void setAuthzObj(String authzObj) {
      this.authzObj = authzObj;
    }

    public PathSeg createChild(String segment) {
      segment = segment.intern();
      PathSeg child = new PathSeg(this, segment);
      children.put(segment, child);
      return child;
    }
    public void delete() {
      this.authzObj = null;
      if (children.isEmpty() && (parent != null)) {
        parent.children.remove(this);
        parent.deleteIfDangling();
      }
    }
    private void deleteIfDangling() {
      if ((children.isEmpty())&&(authzObj == null)) {
        delete();
      }
    }

    public String getFullPath() {
      return getFullPath(new HashMap<PathSeg, String>());
    }

    /**
     * This function will generally be called while creating a full
     * image. This will traverse the <code>authzObjToPath</code> mapping
     * and generate path strings for each for the path segments. Since the
     * full path is a recursive call, The function requires the caller to
     * provide a "memoization" map that store already constructed values
     * thereby reducing the need to un-necessarily recurse.
     * @param pathCache
     * @return
     */
    private String getFullPath(Map<PathSeg, String> pathCache) {
      if (pathCache.containsKey(this)) {
        return pathCache.get(this);
      } else {
        if (parent == null) {
          pathCache.put(this, segment);
          return segment;
        } else {
          String retVal = parent.getFullPath(pathCache) + Path.SEPARATOR
              + segment;
          pathCache.put(this, retVal);
          return retVal;
        }
      }
    }
  }

  private final LoadingCache<String, List<PathSeg>> authzObjToPath;
  // prefix SHOULD end with '/'
  // All createChild and delete() operations must synchronize on
  // the root of the tree;
  private volatile PathSeg rootPath;
  private final MetastoreClient metastoreClient;

  public HMSPathCache(MetastoreClient metastoreClient, long cacheExpiry,
      int updateLogSize, String prefix) {
    super(updateLogSize);
    this.metastoreClient = metastoreClient;
    authzObjToPath = CacheBuilder.newBuilder()
        .expireAfterAccess(cacheExpiry, TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener<String, List<PathSeg>>() {
          @Override
          public void onRemoval(
              RemovalNotification<String, List<PathSeg>> notification) {
            List<PathSeg> paths = notification.getValue();
            for (PathSeg path : paths) {
              path.delete();
            }
          }
        }).build(new CacheLoader<String, List<PathSeg>>() {
          @Override
          public List<PathSeg> load(String key) throws Exception {
            return new ArrayList<PathSeg>();
          }
        }); 
    rootPath = new PathSeg(null, prefix);
  }

  public String serializeAllPaths() {
    StringBuilder sb = new StringBuilder();
    synchronized (rootPath) {
      serializePaths(sb, rootPath);
    }
    return sb.toString();
  }

  private void serializePaths(StringBuilder sb, PathSeg path) {
    sb.append(path.segment);
    if (path.authzObj != null) {
      sb.append("#").append(path.authzObj);
    }
    sb.append("[");
    if (path.children.size() > 0) {
      for (PathSeg child : path.children.values()) {
        serializePaths(sb, child);
      }
    }
    sb.append("]");
  }

  public PathSeg deserializeAllPaths(String pathSer, String prefix) {
    PathSeg tempRoot = new PathSeg(null, "");
    deserializePaths(0, pathSer, tempRoot);
    return tempRoot.children.get("/");
  }

  // For efficiency, we pass in a start index.. this should prevent having
  // to do substrings to extract sections of the original serialized pathTree.
  // NOTE : the substring() used inside the method is used to extract the
  //        actual token (the authzObj name / segment name) which in any
  //        case will be interned and unused objects will be GC-ed.
  private int deserializePaths(int start, String pathSer, PathSeg parent) {
    while ((start < pathSer.length())&&(pathSer.charAt(start) != ']')) {
      int childStart = pathSer.indexOf("[", start);
      int i = pathSer.indexOf("#", start);
      PathSeg pathSeg = null;
      if ((i > 0)&&(i < childStart)) {
        pathSeg = parent.createChild(pathSer.substring(start, i));
        pathSeg.setAuthzObj(pathSer.substring(i + 1, childStart));
        try {
          List<PathSeg> paths = authzObjToPath.get(pathSeg.authzObj);
          paths.add(pathSeg);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
      } else {
        pathSeg = parent.createChild(pathSer.substring(start, childStart));
      }
      start = deserializePaths(childStart + 1, pathSer, pathSeg);
    }
    return start + 1;
  }

  public void removePathsForAuthzObj(String authzObj, String... paths) {
    for (String path : paths) {
      path = cleanPath(path);
      synchronized (rootPath) {
        PathSeg parent = rootPath;
        String subPath = path.substring(parent.segment.length());
        while ((subPath != null) && !subPath.isEmpty()) {
          int i = subPath.indexOf(Path.SEPARATOR);
          if (i > 0) {
            String currSeg = subPath.substring(0, i);
            currSeg = currSeg.intern();
            PathSeg currPathSeg = parent.children.get(currSeg);
            if (currPathSeg == null) {
              // NO such child
              break;
            }
            subPath = subPath.substring(i + 1);
            parent = currPathSeg;
          } else {
            // leaf
            PathSeg leafPathSeg = parent.children.get(subPath.intern());
            if (leafPathSeg != null) {
              leafPathSeg.delete();
            }
            break;
          }
        }
      }
    }
  }

  public void addPathsForAuthzObj(String authzObj, String... paths) {
    for (String path : paths) {
      // Sometimes 'null'
      if (path == null) continue;
      path = cleanPath(path);
      synchronized (rootPath) {
        PathSeg parent = rootPath;
        String subPath = path.substring(parent.segment.length());
        while ((subPath != null) && !subPath.isEmpty()) {
          int i = subPath.indexOf(Path.SEPARATOR);
          if (i > 0) {
            String currSeg = subPath.substring(0, i);
            currSeg = currSeg.intern();
            PathSeg currPathSeg = parent.children.get(currSeg);
            if (currPathSeg == null) {
              currPathSeg = parent.createChild(currSeg);
            }
            subPath = subPath.substring(i + 1);
            parent = currPathSeg;
          } else {
            // leaf
            PathSeg ch = parent.createChild(subPath.intern());
            try {
              ch.setAuthzObj(authzObj);
              List<PathSeg> pList = authzObjToPath.get(ch.segment);
              pList.add(ch);
            } catch (ExecutionException ex) {
              throw new RuntimeException(ex);
            }
            break;
          }
        }
      }
    }
  }

  /**
   * Retrieve all objects related to this path (associated with the same
   * authzObj). For eg. If a request is made for a path that is a partition,
   * The function will return all paths of ALL partitions associated with
   * the table
   * The 'exactMath' parameter is used to signal the Cache to return the most
   * valid parent path that is associated with an authzObject and return all
   * the paths associated with that object.
   * @param path
   * @param exactMatch
   * @return
   */
  public Map<String, LinkedList<String>> getAllRelatedPaths(String path,
      boolean exactMatch) {
    HashMap<String, LinkedList<String>> retMap =
        new HashMap<String, LinkedList<String>>();
    List<PathSeg> pathList = null;
    String authzObj = null;
    synchronized (rootPath) {
      try {
        authzObj = retrieveAuthzObjFromPath(path, exactMatch);
        if (authzObj != null) {
          // get clone of list
          pathList = new ArrayList<PathSeg>(authzObjToPath.get(authzObj));
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      if ((pathList != null) && (!pathList.isEmpty())) {
        LinkedList<String> pList = new LinkedList<String>();
        retMap.put(authzObj, pList);
        for (PathSeg pSeg : pathList) {
          pList.add(pSeg.getFullPath());
        }
      }
      return retMap;
    }
  }

  public String retrieveAuthzObjFromPath(String path, boolean onlyExactMatch) {
    PathSeg parent = rootPath;
    String subPath = cleanPath(path).substring(parent.segment.length());
    PathSeg mostValidParent = null;
    String retVal = null;
    while ((subPath != null) && !subPath.isEmpty()) {
      int i = subPath.indexOf(Path.SEPARATOR);
      if (parent.authzObj != null) {
        mostValidParent = parent;
      }
      if (i > 0) {
        String currSeg = subPath.substring(0, i);
        PathSeg currPathSeg = parent.children.get(currSeg.intern());
        // No such dir exists
        if (currPathSeg == null) {
          retVal = null;
          break;
        }
        subPath = subPath.substring(i + 1);
        parent = currPathSeg;
      } else {
        PathSeg leafPathSeg = parent.children.get(subPath.intern());
        if ((leafPathSeg == null) || (leafPathSeg.authzObj == null)) {
          if (onlyExactMatch) {
            retVal = null;
          } else {
            retVal =
                (mostValidParent != null) ? mostValidParent.authzObj : null;
          }
        } else {
          retVal = leafPathSeg.authzObj;
        }
        break;
      }
    }
    // touch the cache;
    if (retVal != null) {
      authzObjToPath.getIfPresent(retVal);
    }
    return retVal;
  }

  @Override
  protected HMSUpdate retrieveFullImageFromSourceAndApply(long seqNum) {
    synchronized (rootPath) {
      authzObjToPath.invalidateAll();
      rootPath.children.clear();
      List<Database> allDatabases = metastoreClient.getAllDatabases();
      for (Database db : allDatabases) {
        addPathsForAuthzObj(db.getName(), db.getLocationUri());
        List<Table> allTables = metastoreClient.getAllTablesOfDatabase(db);
        for (Table tbl : allTables) {
          List<Partition> tblParts = metastoreClient.listAllPartitions(db, tbl);
          String[] partPaths = new String[tblParts.size() + 1];
          partPaths[0] = tbl.getSd().getLocation();
          int i = 1;
          for (Partition part : tblParts) {
            partPaths[i++] = part.getSd().getLocation();
          }
          addPathsForAuthzObj(tbl.getDbName() + "." + tbl.getTableName(),
              partPaths);
        }
      }
    }
    return new HMSUpdate(seqNum, serializeAllPaths());
  }

  @Override
  protected void applyFullImageUpdate(final HMSUpdate hmsUpdate) {
    authzObjToPath.invalidateAll();
    synchronized (rootPath) {
      rootPath = deserializeAllPaths(hmsUpdate.getPathDump(),
          rootPath.segment);
    }
  }

  @Override
  protected void applyPartialUpdate(HMSUpdate hmsUpdate) {
    for (PathUpdate pathUpdate : hmsUpdate.getPathUpdates()) {
      addPathsForAuthzObj(pathUpdate.getAuthzObj(), pathUpdate
          .getAddPaths().toArray(new String[0]));
      List<String> delPaths = pathUpdate.getDelPaths();
      if ((delPaths.size() == 1)
          && (delPaths.get(0).equals(HMSUpdate.ALL_PATHS))) {
        // Remove all paths.. eg. drop table
        try {
          List<PathSeg> existingPaths = authzObjToPath.get(pathUpdate
              .getAuthzObj());
          if (existingPaths.size() > 0) {
            removePathsForAuthzObj(pathUpdate.getAuthzObj(),
                existingPaths.toArray(new String[0]));
          }
        } catch (ExecutionException e) {
          new RuntimeException(e);
        }
      } else {
        removePathsForAuthzObj(pathUpdate.getAuthzObj(), pathUpdate
            .getDelPaths().toArray(new String[0]));
      }
    }
  }

  @Override
  protected HMSUpdate createFullImageUpdate(long seqNum) {
    HMSUpdate fullImage = new HMSUpdate(seqNum, serializeAllPaths());
    return fullImage;
  }

  private String cleanPath(String path) {
    try {
      return new URI(path).getPath();
    } catch (URISyntaxException e) {
      throw new RuntimeException("Incomprehensible path [" + path + "]");
    }
  }

}
