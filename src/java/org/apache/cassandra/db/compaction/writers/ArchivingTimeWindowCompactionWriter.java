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

import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

public class ArchivingTimeWindowCompactionWriter extends CompactionAwareWriter
{
    public ArchivingTimeWindowCompactionWriter(final ColumnFamilyStore cfs, final Directories directories,
                                               final LifecycleTransaction txn, final Set<SSTableReader> nonExpiredSSTables,
                                               boolean keepOriginals, final boolean useArchiveDirectory)
    {
        super(cfs, directories, txn, nonExpiredSSTables, keepOriginals, useArchiveDirectory);
        long totalSize = cfs.getExpectedCompactedFileSize(txn.originals(), OperationType.COMPACTION);

        //This one uses the directory based on whether TWCS has decided it is archive or not
        @SuppressWarnings("resource")
        SSTableWriter writer = SSTableWriter.create(cfs.newSSTableDescriptor(getDirectories().getLocationForDisk(getDirectories().getWriteableLocation(totalSize, useArchiveDirectory))),
                                                    estimatedTotalKeys,
                                                    minRepairedAt,
                                                    pendingRepair,
                                                    cfs.metadata,
                                                    new MetadataCollector(txn.originals(), cfs.metadata().comparator, 0),
                                                    SerializationHeader.make(cfs.metadata(), nonExpiredSSTables),
                                                    cfs.indexManager.listIndexes(),
                                                    txn);
        sstableWriter.switchWriter(writer);
    }

    protected boolean realAppend(UnfilteredRowIterator partition)
    {
        return sstableWriter.append(partition) != null;
    }

    protected void switchCompactionLocation(Directories.DataDirectory directory)
    {
        //This one switches the directory if the archive directory happens to be JBOD as well
        @SuppressWarnings("resource")
        SSTableWriter writer = SSTableWriter.create(cfs.newSSTableDescriptor(getDirectories().getLocationForDisk(directory)),
                                                    estimatedTotalKeys,
                                                    minRepairedAt,
                                                    pendingRepair,
                                                    cfs.metadata,
                                                    new MetadataCollector(txn.originals(), cfs.metadata().comparator, 0),
                                                    SerializationHeader.make(cfs.metadata(), nonExpiredSSTables),
                                                    cfs.indexManager.listIndexes(),
                                                    txn);
        sstableWriter.switchWriter(writer);
    }
}
