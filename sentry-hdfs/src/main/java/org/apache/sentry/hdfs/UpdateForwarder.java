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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public abstract class UpdateForwarder<K extends UpdateForwarder.Update> {

  public interface Update {
    boolean hasFullImage();
    long getSeqNum();
  }

  private final AtomicLong lastSeenSeqNum = new AtomicLong(0);
  private final AtomicLong lastCommittedSeqNum = new AtomicLong(0);
  // Updates should be handled in order
  private final Executor updateHandler = Executors.newSingleThreadExecutor();

  // Update log is used when propagate updates to a downstream cache.
  // The update log stores all commits that were applied to this cache.
  // When the update log is filled to capacity (updateLogSize), all
  // entries are cleared and a compact image if the state of the cache is
  // appended to the log.
  // The first entry in an update log (consequently the first update a
  // downstream cache sees) will be a full image. All subsequent entries are
  // partial edits
  private final LinkedList<K> updateLog = new LinkedList<K>();
  // UpdateLog is dissabled when updateLogSize = 0;
  private final int updateLogSize;

  public UpdateForwarder(int updateLogSize) {
    this.updateLogSize = updateLogSize;
  }

  /**
   * Handle notifications from HMS plug-in or upstream Cache
   * @param update
   */
  public void handleUpdateNotification(final K update) {
    final boolean editNotMissed = 
        lastSeenSeqNum.incrementAndGet() == update.getSeqNum();
    if (!editNotMissed) {
      lastSeenSeqNum.set(update.getSeqNum());
    }
    Runnable task = new Runnable() {
      @Override
      public void run() {
        K toUpdate = update;
        if (update.hasFullImage()) {
          // Get full image from the update and apply
          // will be used by downstream caches
          applyFullImageUpdate(update);
        } else {
          if (editNotMissed) {
            // apply partial update
            applyPartialUpdate(update);
          } else {
            toUpdate = retrieveFullImageFromSourceAndApply(update.getSeqNum());
          }
        }
        appendToUpdateLog(toUpdate);
      }
    };
    updateHandler.execute(task);
  }

  private void appendToUpdateLog(K update) {
    synchronized (updateLog) {
      if (updateLogSize > 0) {
        if (update.hasFullImage() || (updateLog.size() == updateLogSize)) {
          // Essentially a log compaction
          updateLog.clear();
          updateLog.add(update.hasFullImage() ? update
              : createFullImageUpdate(update.getSeqNum()));
        } else {
          updateLog.add(update);
        }
      }
      lastCommittedSeqNum.set(update.getSeqNum());
    }
  }

  /**
   * Return all updates from requested seqNum (inclusive)
   * @param seqNum
   * @return
   */
  public List<K> getAllUpdatesFrom(long seqNum) {
    List<K> retVal = new LinkedList<K>();
    long currSeqNum = lastCommittedSeqNum.get();
    if ((updateLogSize == 0) || (seqNum > currSeqNum)) {
      // If cache not configured with an updateLog
      // or if caller already has latest updates
      return retVal;
    }
    synchronized (updateLog) {
      K head = updateLog.peek();
      if ((head == null)||(head.getSeqNum() > seqNum)) {
        // Caller has diverged greatly..
        if ((head != null)&&(head.hasFullImage())) {
          // head is a refresh(full) image
          // Send full image along with partial updates
          for (K u : updateLog) {
            retVal.add(u);
          }
        } else {
          // Create a full image
          // clear updateLog
          // add fullImage to head of Log
          // NOTE : This should ideally never happen
          //        except in the rare case that a downStream cache
          //        is requesting before this is initialized
          K fullImage = createFullImageUpdate(currSeqNum);
          updateLog.clear();
          updateLog.add(fullImage);
          retVal.add(fullImage);
        }
      } else {
        // increment iterator to requested seqNum
        Iterator<K> iter = updateLog.iterator();
        K u = null;
        while (iter.hasNext()) {
          u = iter.next();
          if (u.getSeqNum() == seqNum) {
            break;
          }
        }
        // add all updates from requestedSeq
        // to committedSeqNum
        for (long seq = seqNum; seq <= currSeqNum; seq ++) {
          retVal.add(u);
          if (iter.hasNext()) {
            u = iter.next();
          } else {
            break;
          }
        }
      }
    }
    return retVal;
  }
 
  boolean areAllUpdatesCommited() {
    return lastCommittedSeqNum.get() == lastSeenSeqNum.get();
  }

  long getLastCommitted() {
    return lastCommittedSeqNum.get();
  }

  protected abstract K createFullImageUpdate(long currSeqNum);

  protected abstract K retrieveFullImageFromSourceAndApply(long currSeqNum);

  protected abstract void applyFullImageUpdate(K update);

  protected abstract void applyPartialUpdate(K update);

}
