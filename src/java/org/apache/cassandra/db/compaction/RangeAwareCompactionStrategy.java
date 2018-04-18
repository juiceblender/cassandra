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

import java.net.InetAddress;
import java.util.*;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

/**
 * Compaction strategy that aims to have a set of sstables per owned range. For vnodes with 256 tokens and RF=3 this means
 * there will be 768 instances of the actual compaction strategy, each keeping track of the sstables for a single vnode.
 *
 */
public class RangeAwareCompactionStrategy extends AbstractCompactionStrategy implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(RangeAwareCompactionStrategy.class);
    private final Set<SSTableReader> l0sstables = new HashSet<>();
    private final List<AbstractCompactionStrategy> strategies = new ArrayList<>();
    private List<Token> rangeBoundaries = null;
    private final Set<SSTableReader> unknownSSTables = new HashSet<>();
    private final long rangeMinSSTableSize;
    private final SizeTieredCompactionStrategyOptions sizeTieredOptions;
    private List<Range<Token>> localRanges;
    private int estimatedL0compactions = 0;
    private CompactionParams compactionParams;
    private int nextRangeToCompact = 0;
    private volatile boolean needToRefresh = true;

    private final EnumSet<ApplicationState> refreshStates = EnumSet.of(ApplicationState.SCHEMA, ApplicationState.RACK, ApplicationState.TOKENS);


    public RangeAwareCompactionStrategy(ColumnFamilyStore cfs, CompactionParams params)
    {
        super(cfs, params.options());

        rangeMinSSTableSize = params.minRangeSSTableSize();
        compactionParams = params;
        sizeTieredOptions = new SizeTieredCompactionStrategyOptions(options);
    }

    @Override
    public void startup()
    {
        super.startup();
        Gossiper.instance.register(this);
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        Gossiper.instance.unregister(this);
    }

    /**
     * Refreshes the range boundaries - called whenever we get a compaction task but it only actually refreshes the boundaries
     * if it needs it (ie, needToRefresh has been set to true)
     */
    private void refreshRangeBoundaries()
    {
        if (!needToRefresh)
            return;

        Collection<Range<Token>> lr = StorageService.getFutureLocalTokens(cfs);
        if (lr == null || lr.isEmpty())
        {
            logger.warn("Don't know the local ranges yet, not doing any compaction.");
            return;
        }
        needToRefresh = false;
        logger.debug("Refreshing range boundaries");

        List<Range<Token>> newLocalRanges = Range.sort(lr);

        if (localRanges != null && newLocalRanges.equals(localRanges))
        {
            logger.trace("No change in local ranges");
            return;
        }

        localRanges = Range.sort(lr);
        rangeBoundaries = new ArrayList<>();
        for (Range<Token> r : newLocalRanges)
            rangeBoundaries.add(r.right == cfs.getPartitioner().getMinimumToken() ? cfs.getPartitioner().getMaximumToken() : r.right);
        rangeBoundaries.set(rangeBoundaries.size() - 1, cfs.getPartitioner().getMaximumToken());
        strategies.clear();
        for (int i = 0; i < rangeBoundaries.size(); i++)
            strategies.add(null); // we lazily create the actual instances when we need them
        // unknown sstables are the ones added before we knew the range boundaries:
        unknownSSTables.forEach(this::addSSTable);
        unknownSSTables.clear();
    }

    /**
     * Get the next background compaction task, prioritizes doing compaction in L0 over L1 since
     * it is assumed that getting the sstables into the per-range strategies is a good thing. If L0 has many files,
     * we will always hit many files when doing reads. If we can have L0 empty and have more files in the "L1" strategies,
     * we should be able to reduce the number of sstables we touch per read
     *
     * @param gcBefore throw away tombstones older than this
     *
     * @return
     */
    @Override
    @SuppressWarnings("resource")
    public synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        refreshRangeBoundaries();
        if (rangeBoundaries == null || rangeBoundaries.isEmpty())
            return null;
        // need to get both l0 and l1 before checking which to return in order to update estimatedRemainingCompactions
        Iterable<SSTableReader> l0candidates = getL0Candidates();

        if (l0candidates != null)
        {
            LifecycleTransaction txn = cfs.getTracker().tryModify(l0candidates, OperationType.COMPACTION);
            if (txn != null)
                return new RangeAwareCompactionTask(cfs, txn, gcBefore, rangeBoundaries, rangeMinSSTableSize);
        }

        return getL1Candidates(gcBefore);
    }

    /**
     * Get sstables for size tiered compaction in L0
     *
     * @return a bunch of sstables to compact or null if there are no compactions to do in L0
     */
    private Iterable<SSTableReader> getL0Candidates()
    {
        Iterable<SSTableReader> candidates = cfs.getTracker().getUncompacting(l0sstables);
        List<Pair<SSTableReader, Long>> pairs = SizeTieredCompactionStrategy.createSSTableAndLengthPairs(AbstractCompactionStrategy.filterSuspectSSTables(candidates));
        List<List<SSTableReader>> buckets = SizeTieredCompactionStrategy.getBuckets(pairs,
                                                                                    sizeTieredOptions.bucketHigh,
                                                                                    sizeTieredOptions.bucketLow,
                                                                                    sizeTieredOptions.minSSTableSize);
        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();
        estimatedL0compactions = SizeTieredCompactionStrategy.getEstimatedCompactionsByTasks(cfs, buckets);
        Iterable<SSTableReader> interesting = SizeTieredCompactionStrategy.mostInterestingBucket(buckets, minThreshold, maxThreshold);


        if (interesting != null && !Iterables.isEmpty(interesting))
            return interesting;

        return null;
    }

    /**
     * Get a compaction task in L1, here we iterate over the local strategies and if one has a compaction
     * to do, we execute that. For the next call, we will query the next compaction strategy instance for a
     * compaction task.
     *
     * @param gcBefore
     * @return
     */
    private AbstractCompactionTask getL1Candidates(int gcBefore)
    {
        for (int i = 0; i < strategies.size(); i++)
        {
            int toCompactIndex = (i + nextRangeToCompact) % strategies.size();
            AbstractCompactionStrategy strategy = strategies.get(toCompactIndex);
            if (strategy != null)
            {
                AbstractCompactionTask task = strategy.getNextBackgroundTask(gcBefore);

                if (task != null)
                {
                    logger.debug("Compacting L1 range #{} ({} sstables)", toCompactIndex, task.transaction.originals().size());
                    nextRangeToCompact = (toCompactIndex + 1) % strategies.size();
                    return task;
                }
            }
        }
        return null;
    }

    /**
     * todo: create an ordered major compaction: first major compact L0, then major compact each vnode
     *
     * Get sstables for a bunch of major compactions
     *
     * For the L0 major compaction, we will set the range_min_sstable_size to 0 to guarantee that we compact out everything
     * in L0 into the per-range strategies.
     *
     * Note that this major compaction will almost always create 2 sstables per local range since we pick what sstables
     * to compact at the same time. Optimally we would first do the L0 compaction, and then decide what sstables to compact
     * in the per-range instances (see todo above)
     *
     * @param gcBefore throw away tombstones older than this
     *
     * @param splitOutput
     * @return
     */
    @SuppressWarnings("resource")
    @Override
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        refreshRangeBoundaries();
        List<AbstractCompactionTask> compactionTasks = new ArrayList<>();
        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(l0sstables);
        if (!Iterables.isEmpty(filteredSSTables))
        {
            LifecycleTransaction txn = cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
            if (txn != null)
                compactionTasks.add(new RangeAwareCompactionTask(cfs, txn, gcBefore, rangeBoundaries, 0));
        }
        for (AbstractCompactionStrategy strategy : strategies)
        {
            if (strategy != null)
            {
                Collection<AbstractCompactionTask> tasks = strategy.getMaximalTask(gcBefore, splitOutput);
                if (tasks != null)
                    compactionTasks.addAll(tasks);
            }
        }
        return compactionTasks;
    }

    /**
     * Compact the given sstables together
     *
     * If all are in L0, start a new range aware compaction over these, and push all results into the range compaction
     * strategies.
     *
     * Note that all sstables need to be in either L0 or in a single per-range compaction strategy
     *
     * @param sstables SSTables to compact. Must be marked as compacting.
     * @param gcBefore try to throw away tombstones older than this
     *
     * @return
     */
    @SuppressWarnings("resource")
    @Override
    public synchronized AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        if (sstables.isEmpty())
            return null;
        if (l0sstables.containsAll(sstables))
        {
            LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
            return new RangeAwareCompactionTask(cfs, txn, gcBefore, rangeBoundaries, 0);
        }
        SSTableReader first = sstables.iterator().next();
        int firstIndex = getRangeIndex(first.first.getToken());
        if (sstables.stream().allMatch(s -> getRangeIndex(s.first.getToken()) == firstIndex))
            return strategies.get(firstIndex).getUserDefinedTask(sstables, gcBefore);
        else
            throw new RuntimeException("All sstables are not in the same range-compaction strategy");
    }

    @Override
    public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, final int gcBefore, long maxSSTableBytes)
    {
        if (l0sstables.containsAll(txn.originals()))
            return new RangeAwareCompactionTask(cfs, txn, gcBefore, rangeBoundaries, 0);

        SSTableReader first = txn.originals().iterator().next();
        int firstIndex = getRangeIndex(first.first.getToken());
        if (txn.originals().stream().allMatch(s -> getRangeIndex(s.first.getToken()) == firstIndex))
            return strategies.get(firstIndex).getCompactionTask(txn, gcBefore, maxSSTableBytes);
        else
            throw new RuntimeException("All sstables are not in the same range-compaction strategy");
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        // todo: improve this - currently underestimates by quite a lot if we have much data in L0 since
        // todo: we don't have a way of estimating how much data will end up in the per-range compaction
        // todo: strategy instance.
        int remaining = 0;
        for (AbstractCompactionStrategy strategy : strategies)
        {
            if (strategy != null)
                remaining += strategy.getEstimatedRemainingTasks();
        }
        return estimatedL0compactions + remaining;
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return Integer.MAX_VALUE;
    }

    @Override
    public synchronized void addSSTable(SSTableReader added)
    {
        refreshRangeBoundaries();
        // if we get an sstable before we know the range boundaries, we keep it separate and don't do any compaction.
        if (rangeBoundaries == null || rangeBoundaries.isEmpty())
        {
            unknownSSTables.add(added);
            return;
        }

        if (!isSingleRange(added))
            l0sstables.add(added);
        else
            getStrategyFor(added.first.getToken()).addSSTable(added);
    }

    private boolean isSingleRange(SSTableReader added)
    {
        int rangeIndex = getRangeIndex(added.first.getToken());
        if (rangeIndex == rangeBoundaries.size() - 1)
            return true; // the last boundary contains everything to maxtoken
        return rangeBoundaries.get(rangeIndex + 1).compareTo(added.last.getToken()) > 0;
    }

    @Override
    public synchronized void removeSSTable(SSTableReader sstable)
    {
        unknownSSTables.remove(sstable);
        if (rangeBoundaries == null || rangeBoundaries.isEmpty())
            return;

        if (!isSingleRange(sstable))
            l0sstables.remove(sstable);
        else
            getStrategyFor(sstable.first.getToken()).removeSSTable(sstable);
    }

    private AbstractCompactionStrategy getStrategyFor(Token token)
    {
        int strategyIndex = getRangeIndex(token);
        return getStrategyFor(strategyIndex);
    }

    private AbstractCompactionStrategy getStrategyFor(int index)
    {
        if (strategies.get(index) == null)
            strategies.set(index, cfs.createCompactionStrategyInstance(compactionParams));
        return strategies.get(index);
    }

    private int getRangeIndex(Token token)
    {
        int pos = Collections.binarySearch(rangeBoundaries, token);
        if (pos < 0)
            pos = -pos - 1;
        return pos;
    }

    @Override
    public Set<SSTableReader> getSSTables()
    {
        Set<SSTableReader> sstables = new HashSet<>();
        sstables.addAll(l0sstables);
        for (AbstractCompactionStrategy strategy : strategies)
        {
            if (strategy != null)
                Iterables.addAll(sstables, strategy.getSSTables());
        }
        return sstables;
    }

    public int[] getAllLevelSize()
    {
        int[] levelSizes = new int[0];
        for (AbstractCompactionStrategy strategy : strategies)
        {
            if (strategy != null)
                levelSizes = CompactionStrategyManager.sumArrays(levelSizes, strategy.getAllLevelSize());
        }
        if (levelSizes.length > 0)
        {
            System.arraycopy(levelSizes, 0, levelSizes, 1, levelSizes.length - 1);
            levelSizes[0] = l0sstables.size();
        }
        else
            levelSizes = new int[]{0};
        return levelSizes;
    }

    public Map<String, Object> getStrategyDescription()
    {
        Map<String, Object> description = new HashMap<>();
        description.putAll(super.getStrategyDescription());
        List<Map<String, Object>> innerDescription = new ArrayList<>();
        int i = 0;
        for (AbstractCompactionStrategy strategy : strategies)
        {
            Token min = (i == 0 ? cfs.getPartitioner().getMinimumToken() : rangeBoundaries.get(i - 1));
            Range<Token> boundary = new Range<>(min, rangeBoundaries.get(i));
            Map<String, Object> subDescription = new HashMap<>();
            subDescription.put("boundaries", boundary);
            if (strategy != null)
            {
                subDescription.putAll(strategy.getStrategyDescription());
                innerDescription.add(subDescription);
            }
            else
            {
                innerDescription.add(subDescription);
            }
            i++;
        }
        description.put("innerStrategies", innerDescription);
        return description;

    }

    @Override
    public synchronized ScannerList getScanners(Collection<SSTableReader> sstables, Range<Token> range)
    {
        List<ISSTableScanner> scanners = new ArrayList<>();
        // note that we don't split between repaired/unrepaired here, any instance of RangeAwareCompactionStrategy will only see one kind
        List<SSTableReader> l0scanners = new ArrayList<>();
        for (SSTableReader sstable : sstables)
        {
            if (l0sstables.contains(sstable))
                l0scanners.add(sstable);
        }
        scanners.addAll(super.getScanners(l0scanners, range).scanners);
        Iterable<Collection<SSTableReader>> groupedSSTables = groupSSTables(sstables);
        for (Collection<SSTableReader> groupedSSTable : groupedSSTables)
        {
            SSTableReader firstSSTable = groupedSSTable.iterator().next();
            // we use getStrategyFor here, not index directly into strategies since getStrategyFor will create the compaction strategy instance for us if it does not exist yet
            scanners.addAll(getStrategyFor(firstSSTable.first.getToken()).getScanners(groupedSSTable, range).scanners);
        }
        return new ScannerList(scanners);
    }

    @Override
    public synchronized Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        List<SSTableReader> l0togroup = new ArrayList<>();

        for (SSTableReader sstable : sstablesToGroup)
            if (l0sstables.contains(sstable))
                l0togroup.add(sstable);

        Iterable<Collection<SSTableReader>> groupedSSTables = groupSSTables(sstablesToGroup);

        List<Collection<SSTableReader>> anticompactionGrouping = new ArrayList<>();

        anticompactionGrouping.addAll(super.groupSSTablesForAntiCompaction(l0togroup));

        for (Collection<SSTableReader> groupedSSTable : groupedSSTables)
        {
            SSTableReader firstSSTable = groupedSSTable.iterator().next();
            // we use getStrategyFor here, not index directly into strategies since getStrategyFor will create the compaction strategy instance for us if it does not exist yet
            anticompactionGrouping.addAll(getStrategyFor(firstSSTable.first.getToken()).groupSSTablesForAntiCompaction(groupedSSTable));
        }
        return anticompactionGrouping;
    }

    /**
     * Groups the sstables based on what strategy instance they belong to
     *
     * Ignores the l0 sstables
     */
    @SuppressWarnings("unchecked")
    private Iterable<Collection<SSTableReader>> groupSSTables(Iterable<SSTableReader> sstables)
    {
        List<Collection<SSTableReader>> groupedCollection = new ArrayList<>();
        for (ArrayList group : groupSSTablesArray(sstables))
        {
            if (group != null && group.size() > 0)
                groupedCollection.add(group);
        }

        return groupedCollection;
    }

    @SuppressWarnings("unchecked")
    private ArrayList[] groupSSTablesArray(Iterable<SSTableReader> sstables)
    {
        ArrayList[] groupedSSTables = new ArrayList[strategies.size()];

        for (SSTableReader sstable : sstables)
        {
            if (!l0sstables.contains(sstable))
            {
                int index = getRangeIndex(sstable.first.getToken());
                if (groupedSSTables[index] == null)
                    groupedSSTables[index] = new ArrayList();
                groupedSSTables[index].add(sstable);
            }
        }
        return groupedSSTables;
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void replaceSSTables(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {

        List<SSTableReader> removedFromSubStrategies = new ArrayList<>();
        List<SSTableReader> addedToSubStrategies = new ArrayList<>();

        for (SSTableReader removedSSTable : removed)
        {
            if (l0sstables.contains(removedSSTable))
                removeSSTable(removedSSTable);
            else
                removedFromSubStrategies.add(removedSSTable);
        }
        for (SSTableReader addedSSTable : added)
        {
            if (!isSingleRange(addedSSTable))
                addSSTable(addedSSTable);
            else
                addedToSubStrategies.add(addedSSTable);
        }

        ArrayList[] removedGrouped = groupSSTablesArray(removedFromSubStrategies);
        ArrayList[] addedGrouped = groupSSTablesArray(addedToSubStrategies);

        for (int i = 0; i < strategies.size(); i++)
        {
            AbstractCompactionStrategy strategy = getStrategyFor(i);
            if (removedGrouped[i] != null && addedGrouped[i] != null)
                strategy.replaceSSTables(removedGrouped[i], addedGrouped[i]);
            else if (removedGrouped[i] != null)
                removedGrouped[i].forEach(s -> strategy.removeSSTable((SSTableReader)s));
            else if (addedGrouped[i] != null)
                addedGrouped[i].forEach(s -> strategy.addSSTable((SSTableReader)s));
        }


    }
    @Override
    public boolean shouldDefragment()
    {
        Optional<AbstractCompactionStrategy> first = strategies.stream().filter(s -> s != null).findFirst();
        return first.isPresent() && first.get().shouldDefragment();
    }

    @Override
    public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
    {
        needToRefresh = true;
    }

    @Override
    public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {}

    @Override
    public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
        if (refreshStates.contains(state))
            needToRefresh = true;
    }

    @Override
    public void onAlive(InetAddressAndPort endpoint, EndpointState state) {}

    @Override
    public void onDead(InetAddressAndPort endpoint, EndpointState state) {}

    @Override
    public void onRemove(InetAddressAndPort endpoint)
    {
        needToRefresh = true;
    }

    @Override
    public void onRestart(InetAddressAndPort endpoint, EndpointState state) {}
}
