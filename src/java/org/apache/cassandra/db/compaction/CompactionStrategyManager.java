/*
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
package org.apache.cassandra.db.compaction;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.Pair;

/**
 * Manages the compaction strategies.
 *
 * For each directory, a separate compaction strategy instance for both repaired and unrepaired data, and also one instance
 * for each pending repair. This is done to keep the different sets of sstables completely separate.
 *
 * Operations on this class are guarded by a {@link ReentrantReadWriteLock}. This lock performs mutual exclusion on
 * reads and writes to the following variables: {@link this#repaired}, {@link this#unrepaired}, {@link this#isActive},
 * {@link this#params}, {@link this#currentBoundaries}. Whenever performing reads on these variables,
 * the {@link this#readLock} should be acquired. Likewise, updates to these variables should be guarded by
 * {@link this#writeLock}.
 *
 * Whenever the {@link DiskBoundaries} change, the compaction strategies must be reloaded, so in order to ensure
 * the compaction strategy placement reflect most up-to-date disk boundaries, call {@link this#maybeReloadDiskBoundaries()}
 * before acquiring the read lock to acess the strategies.
 *
 */

public class CompactionStrategyManager implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyManager.class);
    public final CompactionLogger compactionLogger;
    private final ColumnFamilyStore cfs;
    private final boolean partitionSSTablesByTokenRange;
    private final Supplier<EnumMap<Directories.DirectoryType, DiskBoundaries>> boundariesSupplier;

    /**
     * Performs mutual exclusion on the variables below
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    /**
     * Variables guarded by read and write lock above
     */
    private final Map<Directories.DirectoryType, List<AbstractCompactionStrategy>> repaired = new HashMap<>();
    private final Map<Directories.DirectoryType, List<AbstractCompactionStrategy>> unrepaired = new HashMap<>();
    private final Map<Directories.DirectoryType, List<PendingRepairManager>> pendingRepairs = new HashMap<>();
    private volatile CompactionParams params;
    private final EnumMap<Directories.DirectoryType, DiskBoundaries> currentBoundaries = new EnumMap<>(Directories.DirectoryType.class);

    //Whether autocompactions are enabled or not
    private volatile boolean enabled = true;
    //Whether the CSM is currently running (set to false when user decides to cancel in progress compactions)
    private volatile boolean isActive = true;

    /*
        We keep a copy of the schema compaction parameters here to be able to decide if we
        should update the compaction strategy in maybeReload() due to an ALTER.

        If a user changes the local compaction strategy and then later ALTERs a compaction parameter,
        we will use the new compaction parameters.
     */
    private volatile CompactionParams schemaCompactionParams;
    private boolean shouldDefragment;
    private boolean supportsEarlyOpen;
    private int fanout;

    public CompactionStrategyManager(ColumnFamilyStore cfs)
    {
        this(cfs, () -> new EnumMap<Directories.DirectoryType, DiskBoundaries>(Directories.DirectoryType.class){{
            put(Directories.DirectoryType.STANDARD, cfs.getDiskBoundaries(Directories.DirectoryType.STANDARD));

            if (DatabaseDescriptor.getAllArchiveDataFileLocations() != null)
                put(Directories.DirectoryType.ARCHIVE, cfs.getDiskBoundaries(Directories.DirectoryType.ARCHIVE));
        }}, cfs.getPartitioner().splitter().isPresent());
    }

    @VisibleForTesting
    public CompactionStrategyManager(ColumnFamilyStore cfs, Supplier<EnumMap<Directories.DirectoryType, DiskBoundaries>> boundariesSupplier,
                                     boolean partitionSSTablesByTokenRange)
    {
        cfs.getTracker().subscribe(this);
        logger.trace("{} subscribed to the data tracker.", this);
        this.cfs = cfs;
        this.compactionLogger = new CompactionLogger(cfs, this);
        this.boundariesSupplier = boundariesSupplier;
        this.partitionSSTablesByTokenRange = partitionSSTablesByTokenRange;
        params = cfs.metadata().params.compaction;
        enabled = params.isEnabled();
        reload(cfs.metadata().params.compaction);
    }

    /**
     * Return the next background task
     *
     * Returns a task for the compaction strategy that needs it the most (most estimated remaining tasks)
     *
     */
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        AbstractCompactionTask task = getNextBackgroundTask(gcBefore, Directories.DirectoryType.STANDARD);

        //Ideally you would check whether TWCS has that option enabled...but then it is protected in the package so not visible.
        //One other option is to create another function isArchivingEnabled(cfs) which checks whether it's TWCS and whether archiveunits == -1, which is public.
        if (task == null && DatabaseDescriptor.getAllArchiveDataFileLocations() != null)
            task = getNextBackgroundTask(gcBefore, Directories.DirectoryType.ARCHIVE);

        return task;
    }

    /*
    Always try to generate compaction tasks from the standard (hot) directory first. Then only generate from the cold.
     */
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore, Directories.DirectoryType directoryType) {
        maybeReloadDiskBoundaries();
        List<PendingRepairManager> pendingRepairsForDirectoryType = pendingRepairs.get(directoryType);
        List<AbstractCompactionStrategy> repairedForDirectoryType = repaired.get(directoryType);
        List<AbstractCompactionStrategy> unrepairedForDirectoryType = unrepaired.get(directoryType);

        readLock.lock();
        try
        {
            if (!isEnabled())
                return null;

            // first try to promote/demote sstables from completed repairs
            ArrayList<Pair<Integer, PendingRepairManager>> pendingRepairManagers = new ArrayList<>(pendingRepairsForDirectoryType.size());
            for (PendingRepairManager pendingRepair : pendingRepairsForDirectoryType)
            {
                int numPending = pendingRepair.getNumPendingRepairFinishedTasks();
                if (numPending > 0)
                {
                    pendingRepairManagers.add(Pair.create(numPending, pendingRepair));
                }
            }
            if (!pendingRepairManagers.isEmpty())
            {
                pendingRepairManagers.sort((x, y) -> y.left - x.left);
                for (Pair<Integer, PendingRepairManager> pair : pendingRepairManagers)
                {
                    AbstractCompactionTask task = pair.right.getNextRepairFinishedTask();
                    if (task != null)
                    {
                        return task;
                    }
                }
            }

            // sort compaction task suppliers by remaining tasks descending
            ArrayList<Pair<Integer, Supplier<AbstractCompactionTask>>> sortedSuppliers = new ArrayList<>(repairedForDirectoryType.size() + unrepairedForDirectoryType.size() + 1);

            for (AbstractCompactionStrategy strategy : repairedForDirectoryType)
                sortedSuppliers.add(Pair.create(strategy.getEstimatedRemainingTasks(), () -> strategy.getNextBackgroundTask(gcBefore)));

            for (AbstractCompactionStrategy strategy : unrepairedForDirectoryType)
                sortedSuppliers.add(Pair.create(strategy.getEstimatedRemainingTasks(), () -> strategy.getNextBackgroundTask(gcBefore)));

            for (PendingRepairManager pending : pendingRepairsForDirectoryType)
                sortedSuppliers.add(Pair.create(pending.getMaxEstimatedRemainingTasks(), () -> pending.getNextBackgroundTask(gcBefore)));

            sortedSuppliers.sort((x, y) -> y.left - x.left);

            // return the first non-null task
            AbstractCompactionTask task;
            Iterator<Supplier<AbstractCompactionTask>> suppliers = Iterables.transform(sortedSuppliers, p -> p.right).iterator();
            assert suppliers.hasNext();

            do
            {
                task = suppliers.next().get();
            }
            while (suppliers.hasNext() && task == null);

            return task;
        }
        finally
        {
            readLock.unlock();
        }
    }

    public boolean isEnabled()
    {
        return enabled && isActive;
    }

    public boolean isActive()
    {
        return isActive;
    }

    public void resume()
    {
        writeLock.lock();
        try
        {
            isActive = true;
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * pause compaction while we cancel all ongoing compactions
     *
     * Separate call from enable/disable to not have to save the enabled-state externally
      */
    public void pause()
    {
        writeLock.lock();
        try
        {
            isActive = false;
        }
        finally
        {
            writeLock.unlock();
        }

    }

    private void startup()
    {
        writeLock.lock();
        try
        {
            for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
            {
                if (sstable.openReason != SSTableReader.OpenReason.EARLY)
                    compactionStrategyFor(sstable).addSSTable(sstable);
            }
            repaired.forEach((dt, cs) -> cs.forEach(AbstractCompactionStrategy::startup));
            unrepaired.forEach((dt, cs) -> cs.forEach(AbstractCompactionStrategy::startup));
            pendingRepairs.forEach((dt, cs) -> cs.forEach(PendingRepairManager::startup));
            shouldDefragment = repaired.get(Directories.DirectoryType.STANDARD).get(0).shouldDefragment();
            supportsEarlyOpen = repaired.get(Directories.DirectoryType.STANDARD).get(0).supportsEarlyOpen();
            fanout = (repaired.get(Directories.DirectoryType.STANDARD).get(0) instanceof LeveledCompactionStrategy) ?
                     ((LeveledCompactionStrategy) repaired.get(Directories.DirectoryType.STANDARD).get(0)).getLevelFanoutSize() :
                     LeveledCompactionStrategy.DEFAULT_LEVEL_FANOUT_SIZE;
        }
        finally
        {
            writeLock.unlock();
        }
        repaired.forEach((dt, cs) -> cs.forEach(AbstractCompactionStrategy::startup));
        unrepaired.forEach((dt, cs) -> cs.forEach(AbstractCompactionStrategy::startup));
        pendingRepairs.forEach((dt, cs) -> cs.forEach(PendingRepairManager::startup));
        if (Stream.concat(repaired.values().stream(), unrepaired.values().stream()).anyMatch(locs -> locs.stream().anyMatch(cs -> cs.logAll)))
            compactionLogger.enable();
    }

    /**
     * return the compaction strategy for the given sstable
     *
     * returns differently based on the repaired status and which vnode the compaction strategy belongs to
     * @param sstable
     * @return
     */
    public AbstractCompactionStrategy getCompactionStrategyFor(SSTableReader sstable)
    {
        maybeReloadDiskBoundaries();
        return compactionStrategyFor(sstable);
    }

    @VisibleForTesting
    protected AbstractCompactionStrategy compactionStrategyFor(SSTableReader sstable)
    {
        // should not call maybeReloadDiskBoundaries because it may be called from within lock
        readLock.lock();
        try
        {
            int index = compactionStrategyIndexFor(sstable);
            Directories.DirectoryType directoryType = Directories.directoryTypeForSSTable(sstable);
            if (sstable.isPendingRepair())
                return pendingRepairs.get(directoryType).get(index).getOrCreate(sstable);
            else if (sstable.isRepaired())
                return repaired.get(directoryType).get(index);
            else
                return unrepaired.get(directoryType).get(index);
        }
        finally
        {
            readLock.unlock();
        }
    }

    /**
     * Get the correct compaction strategy for the given sstable. If the first token starts within a disk boundary, we
     * will add it to that compaction strategy.
     *
     * In the case we are upgrading, the first compaction strategy will get most files - we do not care about which disk
     * the sstable is on currently (unless we don't know the local tokens yet). Once we start compacting we will write out
     * sstables in the correct locations and give them to the correct compaction strategy instance.
     *
     * @param sstable
     * @return
     */
    @VisibleForTesting
    protected int compactionStrategyIndexFor(SSTableReader sstable)
    {
        // should not call maybeReloadDiskBoundaries because it may be called from within lock
        readLock.lock();
        try
        {
            //We only have a single compaction strategy when sstables are not
            //partitioned by token range
            if (!partitionSSTablesByTokenRange)
                return 0;

            return currentBoundaries.get(Directories.directoryTypeForSSTable(sstable)).getDiskIndex(sstable);
        }
        finally
        {
            readLock.unlock();
        }
    }

    @VisibleForTesting
    List<AbstractCompactionStrategy> getRepaired(Directories.DirectoryType directoryType)
    {
        readLock.lock();
        try
        {
            return Lists.newArrayList(repaired.get(directoryType));
        }
        finally
        {
            readLock.unlock();
        }
    }

    @VisibleForTesting
    List<AbstractCompactionStrategy> getUnrepaired(Directories.DirectoryType directoryType)
    {
        readLock.lock();
        try
        {
            return Lists.newArrayList(unrepaired.get(directoryType));
        }
        finally
        {
            readLock.unlock();
        }
    }

    @VisibleForTesting
    List<AbstractCompactionStrategy> getForPendingRepair(UUID sessionID)
    {
        readLock.lock();
        try
        {
            List<AbstractCompactionStrategy> strategies = new ArrayList<>(pendingRepairs.size());
            pendingRepairs.values().stream().flatMap(Collection::stream).forEach(p -> {
                AbstractCompactionStrategy abstractCompactionStrategy = p.get(sessionID);

                if (abstractCompactionStrategy != null)
                    strategies.add(abstractCompactionStrategy);
            });
            return strategies;
        }
        finally
        {
            readLock.unlock();
        }
    }

    @VisibleForTesting
    Set<UUID> pendingRepairs()
    {
        readLock.lock();
        try
        {
            Set<UUID> ids = new HashSet<>();
            pendingRepairs.values().stream().flatMap(Collection::stream).forEach(p -> ids.addAll(p.getSessions()));
            return ids;
        }
        finally
        {
            readLock.unlock();
        }
    }

    public boolean hasDataForPendingRepair(UUID sessionID)
    {
        readLock.lock();
        try
        {
            return Iterables.any(pendingRepairs.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()), prm -> prm.hasDataForSession(sessionID));
        }
        finally
        {
            readLock.unlock();
        }
    }

    public void shutdown()
    {
        writeLock.lock();
        try
        {
            isActive = false;
            repaired.values().stream().flatMap(Collection::stream).forEach(AbstractCompactionStrategy::shutdown);
            unrepaired.values().stream().flatMap(Collection::stream).forEach(AbstractCompactionStrategy::shutdown);
            pendingRepairs.values().stream().flatMap(Collection::stream).forEach(PendingRepairManager::shutdown);
            compactionLogger.disable();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    public void maybeReload(TableMetadata metadata)
    {
        // compare the old schema configuration to the new one, ignore any locally set changes.
        if (metadata.params.compaction.equals(schemaCompactionParams))
            return;

        writeLock.lock();
        try
        {
            // compare the old schema configuration to the new one, ignore any locally set changes.
            if (metadata.params.compaction.equals(schemaCompactionParams))
                return;
            reload(metadata.params.compaction);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * Checks if the disk boundaries changed and reloads the compaction strategies
     * to reflect the most up-to-date disk boundaries.
     *
     * This is typically called before acquiring the {@link this#readLock} to ensure the most up-to-date
     * disk locations and boundaries are used.
     *
     * This should *never* be called inside by a thread holding the {@link this#readLock}, since it
     * will potentially acquire the {@link this#writeLock} to update the compaction strategies
     * what can cause a deadlock.
     */
    //TODO improve this to reload after receiving a notification rather than trying to reload on every operation
    @VisibleForTesting
    protected boolean maybeReloadDiskBoundaries()
    {
        boolean diskBoundaryChanged = false;
        for (DiskBoundaries diskBoundaries : currentBoundaries.values())
        {
            if (diskBoundaries != null)
            {
                if (!diskBoundaries.isOutOfDate())
                    continue;

                writeLock.lock();
                try
                {
                    if (!diskBoundaries.isOutOfDate())
                        continue;
                    reload(params);
                    diskBoundaryChanged = true;
                }
                finally
                {
                    writeLock.unlock();
                }
            }
        }
        return diskBoundaryChanged;
    }

    /**
     * Reload the compaction strategies
     *
     * Called after changing configuration and at startup.
     * @param newCompactionParams
     */
    private void reload(CompactionParams newCompactionParams)
    {
        boolean enabledWithJMX = enabled && !shouldBeEnabled();
        boolean disabledWithJMX = !enabled && shouldBeEnabled();

        if (currentBoundaries.isEmpty() || currentBoundaries.values().stream().filter(Objects::nonNull).anyMatch(DiskBoundaries::isOutOfDate))
        {
            currentBoundaries.put(Directories.DirectoryType.STANDARD, boundariesSupplier.get().get(Directories.DirectoryType.STANDARD));

            if (DatabaseDescriptor.getAllArchiveDataFileLocations() != null )
                currentBoundaries.put(Directories.DirectoryType.ARCHIVE, boundariesSupplier.get().get(Directories.DirectoryType.ARCHIVE));
        }

        DiskBoundaries diskBoundaries = currentBoundaries.get(Directories.DirectoryType.STANDARD);

        if (diskBoundaries != null)
        {
            if (!newCompactionParams.equals(schemaCompactionParams))
                logger.debug("Recreating compaction strategy - compaction parameters changed for {}.{}", cfs.keyspace.getName(), cfs.getTableName());
            else if (diskBoundaries.isOutOfDate())
                logger.debug("Recreating compaction strategy - disk boundaries are out of date for {}.{}.", cfs.keyspace.getName(), cfs.getTableName());
        }

        setStrategy(newCompactionParams);
        schemaCompactionParams = cfs.metadata().params.compaction;

        if (disabledWithJMX || !shouldBeEnabled() && !enabledWithJMX)
            disable();
        else
            enable();
        startup();
    }

    public int getUnleveledSSTables()
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            if (repaired.get(Directories.DirectoryType.STANDARD).get(0) instanceof LeveledCompactionStrategy && unrepaired.get(Directories.DirectoryType.STANDARD).get(0) instanceof LeveledCompactionStrategy)
            {
                int count = 0;
                for (Directories.DirectoryType directoryType : Directories.DirectoryType.values())
                {
                    for (AbstractCompactionStrategy strategy : repaired.get(directoryType))
                        count += ((LeveledCompactionStrategy) strategy).getLevelSize(0);
                    for (AbstractCompactionStrategy strategy : unrepaired.get(directoryType))
                        count += ((LeveledCompactionStrategy) strategy).getLevelSize(0);
                    for (PendingRepairManager pendingManager : pendingRepairs.get(directoryType))
                        for (AbstractCompactionStrategy strategy : pendingManager.getStrategies())
                            count += ((LeveledCompactionStrategy) strategy).getLevelSize(0);
                }
                return count;
            }
        }
        finally
        {
            readLock.unlock();
        }
        return 0;
    }

    public int getLevelFanoutSize()
    {
        return fanout;
    }

    public int[] getSSTableCountPerLevel()
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            if (repaired.get(Directories.DirectoryType.STANDARD).get(0) instanceof LeveledCompactionStrategy && unrepaired.get(Directories.DirectoryType.STANDARD).get(0) instanceof LeveledCompactionStrategy)
            {
                int[] res = new int[LeveledManifest.MAX_LEVEL_COUNT];
                for (Directories.DirectoryType directoryType : Directories.DirectoryType.values())
                {
                    for (AbstractCompactionStrategy strategy : repaired.get(directoryType))
                    {
                        int[] repairedCountPerLevel = ((LeveledCompactionStrategy) strategy).getAllLevelSize();
                        res = sumArrays(res, repairedCountPerLevel);
                    }
                    for (AbstractCompactionStrategy strategy : unrepaired.get(directoryType))
                    {
                        int[] unrepairedCountPerLevel = ((LeveledCompactionStrategy) strategy).getAllLevelSize();
                        res = sumArrays(res, unrepairedCountPerLevel);
                    }
                    for (PendingRepairManager pending : pendingRepairs.get(directoryType))
                    {
                        int[] pendingRepairCountPerLevel = pending.getSSTableCountPerLevel();
                        res = sumArrays(res, pendingRepairCountPerLevel);
                    }
                }
                return res;
            }
        }
        finally
        {
            readLock.unlock();
        }
        return null;
    }

    static int[] sumArrays(int[] a, int[] b)
    {
        int[] res = new int[Math.max(a.length, b.length)];
        for (int i = 0; i < res.length; i++)
        {
            if (i < a.length && i < b.length)
                res[i] = a[i] + b[i];
            else if (i < a.length)
                res[i] = a[i];
            else
                res[i] = b[i];
        }
        return res;
    }

    public boolean shouldDefragment()
    {
        return shouldDefragment;
    }

    private void handleFlushNotification(Iterable<SSTableReader> added)
    {
        // If reloaded, SSTables will be placed in their correct locations
        // so there is no need to process notification
        if (maybeReloadDiskBoundaries())
            return;

        readLock.lock();
        try
        {
            for (SSTableReader sstable : added)
                compactionStrategyFor(sstable).addSSTable(sstable);
        }
        finally
        {
            readLock.unlock();
        }
    }

    //Used when CF is invalidated, or when SSTables are reloaded(?)
    //Not sure if added and removed can ever be from different directories
    private void handleListChangedNotification(Iterable<SSTableReader> added, Iterable<SSTableReader> removed)
    {
        // If reloaded, SSTables will be placed in their correct locations
        // so there is no need to process notification
        if (maybeReloadDiskBoundaries())
            return;

        final Directories.DirectoryType directoryType;
        if (Iterables.size(added) != 0)
            directoryType = Directories.directoryTypeForSSTable(added.iterator().next());
        else if (Iterables.size(removed) != 0)
            directoryType = Directories.directoryTypeForSSTable(removed.iterator().next());
        else
            return;

        readLock.lock();
        try
        {
            // a bit of gymnastics to be able to replace sstables in compaction strategies
            // we use this to know that a compaction finished and where to start the next compaction in LCS
            Directories.DataDirectory[] locations = cfs.getDirectories().getWriteableLocations(directoryType);
            int locationSize = cfs.getPartitioner().splitter().isPresent() ? locations.length : 1;

            List<Set<SSTableReader>> pendingRemoved = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> pendingAdded = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> repairedRemoved = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> repairedAdded = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> unrepairedRemoved = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> unrepairedAdded = new ArrayList<>(locationSize);

            for (int i = 0; i < locationSize; i++)
            {
                pendingRemoved.add(new HashSet<>());
                pendingAdded.add(new HashSet<>());
                repairedRemoved.add(new HashSet<>());
                repairedAdded.add(new HashSet<>());
                unrepairedRemoved.add(new HashSet<>());
                unrepairedAdded.add(new HashSet<>());
            }

            for (SSTableReader sstable : removed)
            {
                int i = compactionStrategyIndexFor(sstable);
                if (sstable.isPendingRepair())
                    pendingRemoved.get(i).add(sstable);
                else if (sstable.isRepaired())
                    repairedRemoved.get(i).add(sstable);
                else
                    unrepairedRemoved.get(i).add(sstable);
            }
            for (SSTableReader sstable : added)
            {
                int i = compactionStrategyIndexFor(sstable);
                if (sstable.isPendingRepair())
                    pendingAdded.get(i).add(sstable);
                else if (sstable.isRepaired())
                    repairedAdded.get(i).add(sstable);
                else
                    unrepairedAdded.get(i).add(sstable);
            }

            for (int i = 0; i < locationSize; i++)
            {

                if (!pendingRemoved.get(i).isEmpty())
                {
                    pendingRepairs.get(directoryType).get(i).replaceSSTables(pendingRemoved.get(i), pendingAdded.get(i));
                }
                else
                {
                    PendingRepairManager pendingManager = pendingRepairs.get(directoryType).get(i);
                    pendingAdded.get(i).forEach(pendingManager::addSSTable);
                }

                if (!repairedRemoved.get(i).isEmpty())
                    repaired.get(directoryType).get(i).replaceSSTables(repairedRemoved.get(i), repairedAdded.get(i));
                else
                    repaired.get(directoryType).get(i).addSSTables(repairedAdded.get(i));

                if (!unrepairedRemoved.get(i).isEmpty())
                    unrepaired.get(directoryType).get(i).replaceSSTables(unrepairedRemoved.get(i), unrepairedAdded.get(i));
                else
                    unrepaired.get(directoryType).get(i).addSSTables(unrepairedAdded.get(i));
            }
        }
        finally
        {
            readLock.unlock();
        }
    }

    private void handleRepairStatusChangedNotification(Iterable<SSTableReader> sstables)
    {
        if (Iterables.size(sstables) == 0)
            return;

        // If reloaded, SSTables will be placed in their correct locations
        // so there is no need to process notification
        if (maybeReloadDiskBoundaries())
            return;

        final Directories.DirectoryType directoryType = Directories.directoryTypeForSSTable(sstables.iterator().next());
        // we need a write lock here since we move sstables from one strategy instance to another
        readLock.lock();
        try
        {
            for (SSTableReader sstable : sstables)
            {
                int index = compactionStrategyIndexFor(sstable);
                if (sstable.isPendingRepair())
                {
                    pendingRepairs.get(directoryType).get(index).addSSTable(sstable);
                    unrepaired.get(directoryType).get(index).removeSSTable(sstable);
                    repaired.get(directoryType).get(index).removeSSTable(sstable);
                }
                else if (sstable.isRepaired())
                {
                    pendingRepairs.get(directoryType).get(index).removeSSTable(sstable);
                    unrepaired.get(directoryType).get(index).removeSSTable(sstable);
                    repaired.get(directoryType).get(index).addSSTable(sstable);
                }
                else
                {
                    pendingRepairs.get(directoryType).get(index).removeSSTable(sstable);
                    repaired.get(directoryType).get(index).removeSSTable(sstable);
                    unrepaired.get(directoryType).get(index).addSSTable(sstable);
                }
            }
        }
        finally
        {
            readLock.unlock();
        }
    }

    private void handleDeletingNotification(SSTableReader deleted)
    {
        // If reloaded, SSTables will be placed in their correct locations
        // so there is no need to process notification
        if (maybeReloadDiskBoundaries())
            return;
        readLock.lock();
        try
        {
            getCompactionStrategyFor(deleted).removeSSTable(deleted);
        }
        finally
        {
            readLock.unlock();
        }
    }

    public void handleNotification(INotification notification, Object sender)
    {
        if (notification instanceof SSTableAddedNotification)
        {
            handleFlushNotification(((SSTableAddedNotification) notification).added);
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification listChangedNotification = (SSTableListChangedNotification) notification;
            handleListChangedNotification(listChangedNotification.added, listChangedNotification.removed);
        }
        else if (notification instanceof SSTableRepairStatusChanged)
        {
            handleRepairStatusChangedNotification(((SSTableRepairStatusChanged) notification).sstables);
        }
        else if (notification instanceof SSTableDeletingNotification)
        {
            handleDeletingNotification(((SSTableDeletingNotification) notification).deleting);
        }
    }

    public void enable()
    {
        writeLock.lock();
        try
        {
            // enable this last to make sure the strategies are ready to get calls.
            enabled = true;
        }
        finally
        {
            writeLock.unlock();
        }
    }

    public void disable()
    {
        writeLock.lock();
        try
        {
            enabled = false;
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * Create ISSTableScanners from the given sstables
     *
     * Delegates the call to the compaction strategies to allow LCS to create a scanner
     * @param sstables
     * @param ranges
     * @return
     */
    @SuppressWarnings("resource")
    public AbstractCompactionStrategy.ScannerList maybeGetScanners(Collection<SSTableReader> sstables,  Collection<Range<Token>> ranges)
    {
        maybeReloadDiskBoundaries();
        List<ISSTableScanner> scanners = new ArrayList<>(sstables.size());

        if (sstables.size() == 0)
            return new AbstractCompactionStrategy.ScannerList(scanners);

        final Directories.DirectoryType directoryType = Directories.directoryTypeForSSTable(sstables.iterator().next());
        readLock.lock();
        try
        {
            assert repaired.size() == unrepaired.size();
            assert repaired.size() == pendingRepairs.size();

            int numRepaired = repaired.size();
            List<Set<SSTableReader>> pendingSSTables = new ArrayList<>(numRepaired);
            List<Set<SSTableReader>> repairedSSTables = new ArrayList<>(numRepaired);
            List<Set<SSTableReader>> unrepairedSSTables = new ArrayList<>(numRepaired);

            for (int i = 0; i < numRepaired; i++)
            {
                pendingSSTables.add(new HashSet<>());
                repairedSSTables.add(new HashSet<>());
                unrepairedSSTables.add(new HashSet<>());
            }

            for (SSTableReader sstable : sstables)
            {
                int idx = compactionStrategyIndexFor(sstable);
                if (sstable.isPendingRepair())
                    pendingSSTables.get(idx).add(sstable);
                else if (sstable.isRepaired())
                    repairedSSTables.get(idx).add(sstable);
                else
                    unrepairedSSTables.get(idx).add(sstable);
            }

            for (int i = 0; i < pendingSSTables.size(); i++)
            {
                if (!pendingSSTables.get(i).isEmpty())
                    scanners.addAll(pendingRepairs.get(directoryType).get(i).getScanners(pendingSSTables.get(i), ranges));
            }
            for (int i = 0; i < repairedSSTables.size(); i++)
            {
                if (!repairedSSTables.get(i).isEmpty())
                    scanners.addAll(repaired.get(directoryType).get(i).getScanners(repairedSSTables.get(i), ranges).scanners);
            }
            for (int i = 0; i < unrepairedSSTables.size(); i++)
            {
                if (!unrepairedSSTables.get(i).isEmpty())
                    scanners.addAll(unrepaired.get(directoryType).get(i).getScanners(unrepairedSSTables.get(i), ranges).scanners);
            }
        }
        catch (PendingRepairManager.IllegalSSTableArgumentException e)
        {
            ISSTableScanner.closeAllAndPropagate(scanners, new ConcurrentModificationException(e));
        }
        finally
        {
            readLock.unlock();
        }
        return new AbstractCompactionStrategy.ScannerList(scanners);
    }

    public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables,  Collection<Range<Token>> ranges)
    {
        while (true)
        {
            try
            {
                return maybeGetScanners(sstables, ranges);
            }
            catch (ConcurrentModificationException e)
            {
                logger.debug("SSTable repairedAt/pendingRepaired values changed while getting scanners");
            }
        }
    }

    public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables)
    {
        return getScanners(sstables, null);
    }

    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        maybeReloadDiskBoundaries();
        final Directories.DirectoryType directoryType = Directories.directoryTypeForSSTable(sstablesToGroup.iterator().next());
        readLock.lock();
        try
        {
            Map<Integer, List<SSTableReader>> groups = sstablesToGroup.stream().collect(Collectors.groupingBy(this::compactionStrategyIndexFor));
            Collection<Collection<SSTableReader>> anticompactionGroups = new ArrayList<>();

            for (Map.Entry<Integer, List<SSTableReader>> group : groups.entrySet())
                anticompactionGroups.addAll(unrepaired.get(directoryType).get(group.getKey()).groupSSTablesForAntiCompaction(group.getValue()));
            return anticompactionGroups;
        }
        finally
        {
            readLock.unlock();
        }
    }

    public long getMaxSSTableBytes()
    {
        readLock.lock();
        try
        {
            return unrepaired.get(Directories.DirectoryType.STANDARD).get(0).getMaxSSTableBytes();
        }
        finally
        {
            readLock.unlock();
        }
    }

    public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes)
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            validateForCompaction(txn.originals());
            return compactionStrategyFor(txn.originals().iterator().next()).getCompactionTask(txn, gcBefore, maxSSTableBytes);
        }
        finally
        {
            readLock.unlock();
        }

    }

    private void validateForCompaction(Iterable<SSTableReader> input)
    {
        readLock.lock();
        try
        {
            SSTableReader firstSSTable = Iterables.getFirst(input, null);
            assert firstSSTable != null;
            boolean repaired = firstSSTable.isRepaired();
            int firstIndex = compactionStrategyIndexFor(firstSSTable);
            boolean isPending = firstSSTable.isPendingRepair();
            UUID pendingRepair = firstSSTable.getSSTableMetadata().pendingRepair;
            for (SSTableReader sstable : input)
            {
                if (sstable.isRepaired() != repaired)
                    throw new UnsupportedOperationException("You can't mix repaired and unrepaired data in a compaction");
                if (firstIndex != compactionStrategyIndexFor(sstable))
                    throw new UnsupportedOperationException("You can't mix sstables from different directories in a compaction");
                if (isPending && !pendingRepair.equals(sstable.getSSTableMetadata().pendingRepair))
                    throw new UnsupportedOperationException("You can't compact sstables from different pending repair sessions");
            }
        }
        finally
        {
            readLock.unlock();
        }
    }

    //FIXME what happens when performing a major compaction, should we compact all or parameterize.
    public Collection<AbstractCompactionTask> getMaximalTasks(final int gcBefore, final boolean splitOutput)
    {
        maybeReloadDiskBoundaries();
        // runWithCompactionsDisabled cancels active compactions and disables them, then we are able
        // to make the repaired/unrepaired strategies mark their own sstables as compacting. Once the
        // sstables are marked the compactions are re-enabled
        return cfs.runWithCompactionsDisabled(new Callable<Collection<AbstractCompactionTask>>()
        {
            @Override
            public Collection<AbstractCompactionTask> call()
            {
                List<AbstractCompactionTask> tasks = new ArrayList<>();
                readLock.lock();
                try
                {
                    for (AbstractCompactionStrategy strategy : repaired.get(Directories.DirectoryType.STANDARD))
                    {
                        Collection<AbstractCompactionTask> task = strategy.getMaximalTask(gcBefore, splitOutput);
                        if (task != null)
                            tasks.addAll(task);
                    }
                    for (AbstractCompactionStrategy strategy : unrepaired.get(Directories.DirectoryType.STANDARD))
                    {
                        Collection<AbstractCompactionTask> task = strategy.getMaximalTask(gcBefore, splitOutput);
                        if (task != null)
                            tasks.addAll(task);
                    }

                    for (PendingRepairManager pending : pendingRepairs.get(Directories.DirectoryType.STANDARD))
                    {
                        Collection<AbstractCompactionTask> pendingRepairTasks = pending.getMaximalTasks(gcBefore, splitOutput);
                        if (pendingRepairTasks != null)
                            tasks.addAll(pendingRepairTasks);
                    }
                }
                finally
                {
                    readLock.unlock();
                }
                if (tasks.isEmpty())
                    return null;
                return tasks;
            }
        }, false, false);
    }

    /**
     * Return a list of compaction tasks corresponding to the sstables requested. Split the sstables according
     * to whether they are repaired or not, and by disk location. Return a task per disk location and repair status
     * group.
     *
     * @param sstables the sstables to compact
     * @param gcBefore gc grace period, throw away tombstones older than this
     * @return a list of compaction tasks corresponding to the sstables requested
     */
    public List<AbstractCompactionTask> getUserDefinedTasks(Collection<SSTableReader> sstables, int gcBefore)
    {
        maybeReloadDiskBoundaries();
        List<AbstractCompactionTask> ret = new ArrayList<>();
        Directories.DirectoryType directoryType = Directories.directoryTypeForSSTable(sstables.iterator().next());
        readLock.lock();
        try
        {
            Map<Integer, List<SSTableReader>> repairedSSTables = sstables.stream()
                                                                         .filter(s -> !s.isMarkedSuspect() && s.isRepaired() && !s.isPendingRepair())
                                                                         .collect(Collectors.groupingBy(this::compactionStrategyIndexFor));

            Map<Integer, List<SSTableReader>> unrepairedSSTables = sstables.stream()
                                                                           .filter(s -> !s.isMarkedSuspect() && !s.isRepaired() && !s.isPendingRepair())
                                                                           .collect(Collectors.groupingBy(this::compactionStrategyIndexFor));

            Map<Integer, List<SSTableReader>> pendingSSTables = sstables.stream()
                                                                        .filter(s -> !s.isMarkedSuspect() && s.isPendingRepair())
                                                                        .collect(Collectors.groupingBy(this::compactionStrategyIndexFor));

            for (Map.Entry<Integer, List<SSTableReader>> group : repairedSSTables.entrySet())
                ret.add(repaired.get(directoryType).get(group.getKey()).getUserDefinedTask(group.getValue(), gcBefore));

            for (Map.Entry<Integer, List<SSTableReader>> group : unrepairedSSTables.entrySet())
                ret.add(unrepaired.get(directoryType).get(group.getKey()).getUserDefinedTask(group.getValue(), gcBefore));

            for (Map.Entry<Integer, List<SSTableReader>> group : pendingSSTables.entrySet())
                ret.addAll(pendingRepairs.get(directoryType).get(group.getKey()).createUserDefinedTasks(group.getValue(), gcBefore));

            return ret;
        }
        finally
        {
            readLock.unlock();
        }
    }

    public int getEstimatedRemainingTasks()
    {
        maybeReloadDiskBoundaries();
        int tasks = 0;
        readLock.lock();
        try
        {
            for (Directories.DirectoryType directoryType : Directories.DirectoryType.values())
            {
                for (AbstractCompactionStrategy strategy : repaired.get(directoryType))
                    tasks += strategy.getEstimatedRemainingTasks();
                for (AbstractCompactionStrategy strategy : unrepaired.get(directoryType))
                    tasks += strategy.getEstimatedRemainingTasks();
                for (PendingRepairManager pending : pendingRepairs.get(directoryType))
                    tasks += pending.getEstimatedRemainingTasks();
            }
        }
        finally
        {
            readLock.unlock();
        }
        return tasks;
    }

    public boolean shouldBeEnabled()
    {
        return params.isEnabled();
    }

    public String getName()
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            return unrepaired.get(Directories.DirectoryType.STANDARD).get(0).getName();
        }
        finally
        {
            readLock.unlock();
        }
    }

    public List<List<AbstractCompactionStrategy>> getStrategies()
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            List<AbstractCompactionStrategy> pending = new ArrayList<>();
            pendingRepairs.values().stream().flatMap(Collection::stream).forEach(p -> pending.addAll(p.getStrategies()));
            return Arrays.asList(repaired.values().stream().flatMap(Collection::stream).collect(Collectors.toList()),
                                 unrepaired.values().stream().flatMap(Collection::stream).collect(Collectors.toList()), pending);
        }
        finally
        {
            readLock.unlock();
        }
    }

    public void setNewLocalCompactionStrategy(CompactionParams params)
    {
        logger.info("Switching local compaction strategy from {} to {}}", this.params, params);
        writeLock.lock();
        try
        {
            setStrategy(params);
            if (shouldBeEnabled())
                enable();
            else
                disable();
            startup();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    private void setStrategy(CompactionParams params)
    {
        repaired.values().stream().flatMap(Collection::stream).forEach(AbstractCompactionStrategy::shutdown);
        unrepaired.values().stream().flatMap(Collection::stream).forEach(AbstractCompactionStrategy::shutdown);
        pendingRepairs.values().stream().flatMap(Collection::stream).forEach(PendingRepairManager::shutdown);

        repaired.clear();
        unrepaired.clear();
        pendingRepairs.clear();

        repaired.put(Directories.DirectoryType.STANDARD, new ArrayList<>());
        unrepaired.put(Directories.DirectoryType.STANDARD, new ArrayList<>());
        pendingRepairs.put(Directories.DirectoryType.STANDARD, new ArrayList<>());

        if (DatabaseDescriptor.getAllArchiveDataFileLocations() != null)
            repaired.put(Directories.DirectoryType.ARCHIVE, new ArrayList<>());
            unrepaired.put(Directories.DirectoryType.ARCHIVE, new ArrayList<>());
            pendingRepairs.put(Directories.DirectoryType.ARCHIVE, new ArrayList<>());

        if (partitionSSTablesByTokenRange)
        {
            for (Directories.DirectoryType directoryType : Directories.DirectoryType.values())
            {
                DiskBoundaries diskBoundaries = currentBoundaries.get(directoryType);
                if (diskBoundaries != null)
                {
                    for (int i = 0; i < diskBoundaries.directories.size(); i++)
                    {
                        repaired.get(directoryType).add(cfs.createCompactionStrategyInstance(params));
                        unrepaired.get(directoryType).add(cfs.createCompactionStrategyInstance(params));
                        pendingRepairs.get(directoryType).add(new PendingRepairManager(cfs, params));
                    }
                }
            }
        }
        else
        {
            repaired.get(Directories.DirectoryType.STANDARD).add(cfs.createCompactionStrategyInstance(params));
            unrepaired.get(Directories.DirectoryType.STANDARD).add(cfs.createCompactionStrategyInstance(params));
            pendingRepairs.get(Directories.DirectoryType.STANDARD).add(new PendingRepairManager(cfs, params));

            if (currentBoundaries.get(Directories.DirectoryType.ARCHIVE) != null) {
                repaired.get(Directories.DirectoryType.ARCHIVE).add(cfs.createCompactionStrategyInstance(params));
                unrepaired.get(Directories.DirectoryType.ARCHIVE).add(cfs.createCompactionStrategyInstance(params));
                pendingRepairs.get(Directories.DirectoryType.ARCHIVE).add(new PendingRepairManager(cfs, params));
            }
        }
        this.params = params;
    }

    public CompactionParams getCompactionParams()
    {
        return params;
    }

    public boolean onlyPurgeRepairedTombstones()
    {
        return Boolean.parseBoolean(params.options().get(AbstractCompactionStrategy.ONLY_PURGE_REPAIRED_TOMBSTONES));
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                       long keyCount,
                                                       long repairedAt,
                                                       UUID pendingRepair,
                                                       MetadataCollector collector,
                                                       SerializationHeader header,
                                                       Collection<Index> indexes,
                                                       LifecycleTransaction txn)
    {
        maybeReloadDiskBoundaries();
        Directories.DirectoryType directoryType = Directories.directoryTypeForSSTableDescriptor(descriptor);
        readLock.lock();
        try
        {
            // to avoid creating a compaction strategy for the wrong pending repair manager, we get the index based on where the sstable is to be written
            int index = partitionSSTablesByTokenRange? currentBoundaries.get(directoryType).getBoundariesFromSSTableDirectory(descriptor) : 0;
            if (pendingRepair != ActiveRepairService.NO_PENDING_REPAIR)
                return pendingRepairs.get(directoryType).get(index).getOrCreate(pendingRepair).createSSTableMultiWriter(descriptor, keyCount, ActiveRepairService.UNREPAIRED_SSTABLE, pendingRepair, collector, header, indexes, txn);
            else if (repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE)
                return unrepaired.get(directoryType).get(index).createSSTableMultiWriter(descriptor, keyCount, repairedAt, ActiveRepairService.NO_PENDING_REPAIR, collector, header, indexes, txn);
            else
                return repaired.get(directoryType).get(index).createSSTableMultiWriter(descriptor, keyCount, repairedAt, ActiveRepairService.NO_PENDING_REPAIR, collector, header, indexes, txn);
        }
        finally
        {
            readLock.unlock();
        }
    }

    public boolean isRepaired(AbstractCompactionStrategy strategy)
    {
        return repaired.get(Directories.DirectoryType.STANDARD).contains(strategy);
    }

    //FIXME Check this.
    public List<String> getStrategyFolders(AbstractCompactionStrategy strategy)
    {
        readLock.lock();
        try
        {
            Directories.DataDirectory[] locations = cfs.getDirectories().getWriteableLocations(Directories.DirectoryType.STANDARD);
            Directories.DataDirectory[] archiveLocations = cfs.getDirectories().getWriteableLocations(Directories.DirectoryType.ARCHIVE);
            if (partitionSSTablesByTokenRange)
            {
                int unrepairedIndex = unrepaired.get(Directories.DirectoryType.STANDARD).indexOf(strategy);
                if (unrepairedIndex > 0)
                {
                    if (archiveLocations.length != 0)
                        return Arrays.asList(locations[unrepairedIndex].location.getAbsolutePath(), archiveLocations[unrepairedIndex].location.getAbsolutePath());

                    return Collections.singletonList(locations[unrepairedIndex].location.getAbsolutePath());
                }
                int repairedIndex = repaired.get(Directories.DirectoryType.STANDARD).indexOf(strategy);
                if (repairedIndex > 0)
                {
                    if (archiveLocations.length != 0)
                        return Arrays.asList(locations[repairedIndex].location.getAbsolutePath(), archiveLocations[repairedIndex].location.getAbsolutePath());

                    return Collections.singletonList(locations[repairedIndex].location.getAbsolutePath());
                }
                for (int i = 0; i < pendingRepairs.get(Directories.DirectoryType.STANDARD).size(); i++)
                {
                    PendingRepairManager pending = pendingRepairs.get(Directories.DirectoryType.STANDARD).get(i);
                    if (pending.hasStrategy(strategy))
                    {
                        if (archiveLocations.length != 0)
                            return Arrays.asList(locations[i].location.getAbsolutePath(), archiveLocations[i].location.getAbsolutePath());

                        return Collections.singletonList(locations[i].location.getAbsolutePath());
                    }
                }
            }
            List<String> folders = new ArrayList<>(locations.length + archiveLocations.length);
            for (Directories.DataDirectory location : ArrayUtils.addAll(locations, archiveLocations))
            {
                folders.add(location.location.getAbsolutePath());
            }
            return folders;
        }
        finally
        {
            readLock.unlock();
        }
    }

    public boolean supportsEarlyOpen()
    {
        return supportsEarlyOpen;
    }

    @VisibleForTesting
    List<PendingRepairManager> getPendingRepairManagers()
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            return pendingRepairs.get(Directories.DirectoryType.STANDARD);
        }
        finally
        {
            readLock.unlock();
        }
    }

    /**
     * Mutates sstable repairedAt times and notifies listeners of the change with the writeLock held. Prevents races
     * with other processes between when the metadata is changed and when sstables are moved between strategies.
     */
    public void mutateRepaired(Collection<SSTableReader> sstables, long repairedAt, UUID pendingRepair) throws IOException
    {
        Set<SSTableReader> changed = new HashSet<>();

        writeLock.lock();
        try
        {
            for (SSTableReader sstable: sstables)
            {
                sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, repairedAt, pendingRepair);
                sstable.reloadSSTableMetadata();
                changed.add(sstable);
            }
        }
        finally
        {
            try
            {
                // if there was an exception mutating repairedAt, we should still notify for the
                // sstables that we were able to modify successfully before releasing the lock
                cfs.getTracker().notifySSTableRepairedStatusChanged(changed);
            }
            finally
            {
                writeLock.unlock();
            }
        }
    }
}
