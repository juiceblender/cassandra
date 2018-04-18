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
package org.apache.cassandra.db.compaction.writers;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.Pair;

/**
 * Compaction writer for range aware compaction
 *
 * Basic idea is that whenever we compact a bunch of tables, we check the local range boundaries
 * and create a separate sstable if we estimate that the size of the range-sstable would be larger than rangeMinSSTableSize
 */
public class RangeAwareCompactionWriter extends CompactionAwareWriter
{
    private final SSTableRewriter l1writer;
    private final List<Token> rangeBoundaries;
    private final double compactionGain;
    private final double compressionRatio;
    private final long rangeMinSSTableSize;
    private final double totalSize;
    private SSTableRewriter activeWriter;
    private int rangeIndex = -1;
    private Directories.DataDirectory location;
    private long lastEstimate = 0;
    private long lastStart = 0;

    public RangeAwareCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, List<Token> rangeBoundaries, long rangeMinSSTableSize)
    {
        // note that we cant open early here as we are writing to two different files
        super(cfs, directories, false, txn, nonExpiredSSTables, false);
        txn.permitRedundantTransitions(); // we are writing to several files
        l1writer = SSTableRewriter.constructWithoutEarlyOpening(txn, false, maxAge);
        this.rangeBoundaries = rangeBoundaries;
        compactionGain = SSTableReader.estimateCompactionGain(nonExpiredSSTables);
        compressionRatio = getCompressionRatio(nonExpiredSSTables);
        this.rangeMinSSTableSize = rangeMinSSTableSize;
        totalSize = compactionGain * SSTableReader.getTotalBytes(nonExpiredSSTables);
    }

    @Override
    public boolean realAppend(UnfilteredRowIterator partition)
    {
        maybeSwitchRange(partition.partitionKey());
        activeWriter.append(partition);
        return false;
    }

    @Override
    public void switchCompactionLocation(Directories.DataDirectory location)
    {
        this.location = location;
        logger.debug("Switching compaction location to {}", location.location);
        // note that we only switch the l0 writer if we switch compaction location
        sstableWriter.switchWriter(createWriter(location, estimatedTotalKeys));
        if (l1writer.currentWriter() != null)
            l1writer.switchWriter(createWriter(location, estimatedTotalKeys));
    }

    /**
     * if the key is beyond the current range boundary we might switch the writer.
     *
     * we switch if:
     * * we are currently writing to L0 and the next range is estimated to be larger than rangeMinSSTableSize
     * * we are writing to L1 - we either switch to the L0 writer or create a new L1 file if the range is estimated large enough
     *
     * @param key
     */
    private void maybeSwitchRange(DecoratedKey key)
    {
        if (rangeIndex > -1 && key.getToken().compareTo(rangeBoundaries.get(rangeIndex)) <= 0)
            return;

        while (rangeIndex == -1 || key.getToken().compareTo(rangeBoundaries.get(rangeIndex)) > 0)
            rangeIndex++;

        Token rangeStart = rangeIndex == 0 ? cfs.getPartitioner().getMinimumToken() : rangeBoundaries.get(rangeIndex - 1);
        Token rangeEnd = rangeIndex == rangeBoundaries.size() ? cfs.getPartitioner().getMaximumToken() : rangeBoundaries.get(rangeIndex);
        long estimatedRangeSize = estimateRangeSize(nonExpiredSSTables, new Range<>(rangeStart, rangeEnd));
        if (logger.isTraceEnabled() && lastEstimate != 0 && activeWriter != null)
        {
            long lastWriteSize = activeWriter.currentWriter().getOnDiskFilePointer() - lastStart;
            logger.trace("Last estimate was: {} - actual write size was: {} ({})", lastEstimate, lastWriteSize, (double)lastWriteSize/lastEstimate);
        }

        lastEstimate = estimatedRangeSize;
        if (estimatedRangeSize >= rangeMinSSTableSize)
        {
            long estimatedL1Keys = Math.round(estimatedTotalKeys * estimatedRangeSize / totalSize);
            l1writer.switchWriter(createWriter(location, estimatedL1Keys));
            logger.debug("writing range {} to L1, estimated size = {}, loc = {}, estimatedL1Keys = {}", rangeIndex, estimatedRangeSize, l1writer.currentWriter().getFilename(), estimatedL1Keys);
            activeWriter = l1writer;
        }
        else
        {
            activeWriter = sstableWriter;
            logger.debug("writing range {} to L0, estimated size = {}, loc = {}", rangeIndex, estimatedRangeSize, sstableWriter.currentWriter().getFilename());
        }
        lastStart = activeWriter.currentWriter().getOnDiskFilePointer();
    }

    /**
     * Estimate how big file we would get if we compacted the sstables together within the tokenRange.
     */
    private long estimateRangeSize(Set<SSTableReader> sstables, Range<Token> tokenRange)
    {
        long sum = 0;
        for (SSTableReader sstable : sstables)
        {
            List<Pair<Long, Long>> positions = sstable.getPositionsForRanges(Collections.singletonList(tokenRange));
            for (Pair<Long, Long> pos : positions)
                sum += pos.right - pos.left;
        }
        double compressionRatio = cfs.metric.compressionRatio.getValue();
        double uncompressedSize = compactionGain * sum;
        return Math.round((compressionRatio > 0d) ? compressionRatio * uncompressedSize : uncompressedSize);
    }

    @Override
    public Throwable doAbort(Throwable accumulate)
    {
        activeWriter = null;
        accumulate = super.doAbort(accumulate);
        return l1writer.abort(accumulate);
    }

    @Override
    protected Throwable doCommit(Throwable accumulate)
    {
        accumulate = super.doCommit(accumulate);
        return l1writer.commit(accumulate);
    }

    @Override
    protected void doPrepare()
    {
        super.doPrepare();
        l1writer.prepareToCommit();
    }

    @Override
    public List<SSTableReader> finish()
    {
        List<SSTableReader> finished = new ArrayList<>();
        finished.addAll(super.finish());
        finished.addAll(l1writer.finished());
        return finished;
    }

    private SSTableWriter createWriter(Directories.DataDirectory directory, long estimatedKeys)
    {
        File sstableDirectory = getDirectories().getLocationForDisk(directory);
        return SSTableWriter.create(cfs.newSSTableDescriptor(sstableDirectory),
                                    estimatedKeys,
                                    minRepairedAt,
                                    null,
                                    cfs.metadata,
                                    new MetadataCollector(txn.originals(), cfs.metadata().comparator, 0),
                                                            SerializationHeader.make(cfs.metadata(), txn.originals()),
                                                            cfs.indexManager.listIndexes(),
                                                            txn);
    }

    private static double getCompressionRatio(Iterable<SSTableReader> sstables)
    {
        double compressed = 0.0;
        double uncompressed = 0.0;

        for (SSTableReader sstable : sstables)
        {
            compressed += sstable.onDiskLength();
            uncompressed += sstable.uncompressedLength();
        }

        if (compressed == uncompressed || uncompressed == 0)
            return 1.0;

        return compressed / uncompressed;

    }
}

