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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategyOptions.ARCHIVE_SSTABLES_SIZE_KEY;
import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategyOptions.ARCHIVE_SSTABLES_UNIT_KEY;
import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY;
import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY;
import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategyOptions.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY;
import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY;
import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategyOptions.UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY;
import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategyTest.prepareCFS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
public class ArchivingCompactionTest
{
    public static final String KEYSPACE1 = "Keyspace1";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        // Disable tombstone histogram rounding for tests
        System.setProperty("cassandra.streaminghistogram.roundseconds", "1");
        System.setProperty(UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY, "true");

        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testArchivingSSTables() throws IOException, InterruptedException
    {
        final ColumnFamilyStore cfs = prepareCFS();

        final ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        final DecoratedKey key = Util.dk("archived");
        new RowUpdateBuilder(cfs.metadata(), System.currentTimeMillis(), key.getKey())
        .clustering("column").add("val", value).build().applyUnsafe();

        cfs.forceBlockingFlush();

        // Verify that the newly flushed SSTable is not present in the archive directory
        // We use max depth of 2 to omit backup/snapshot directories
        assertFalse(Files.walk(Paths.get(DatabaseDescriptor.getAllArchiveDataFileLocations()[0]).resolve(KEYSPACE1), 2).anyMatch(d -> d.toString().contains("Data.db")));
        assertTrue(Files.walk(Paths.get(DatabaseDescriptor.getAllStandardDataFileLocations()[0]).resolve(KEYSPACE1), 2).anyMatch(d -> d.toString().contains("Data.db")));

        TimeWindowCompactionStrategy twcs = createQuicklyExpiringTWCS(cfs, true);
        cfs.getLiveSSTables().forEach(twcs::addSSTable);
        twcs.startup();

        // There's just a single SSTable with no tombstones and it hasn't passed archive time yet, so we expect
        // to have no compaction tasks.
        assertNull(twcs.getNextBackgroundTask(FBUtilities.nowInSeconds()));

        Thread.sleep(4000);
        AbstractCompactionTask t = twcs.getNextBackgroundTask(FBUtilities.nowInSeconds());

        // It's now past the archive time window, so we expect there to be a compaction task to write to archive
        assertNotNull(t);
        t.execute(null);

        assertTrue(Files.walk(Paths.get(DatabaseDescriptor.getAllArchiveDataFileLocations()[0]).resolve(KEYSPACE1), 2).anyMatch(d -> d.toString().contains("Data.db")));
    }

    @Test
    public void testTWCSCompactionGetsPriorityFirstOverArchiving() throws IOException, InterruptedException
    {
        final ColumnFamilyStore cfs = prepareCFS();
        createAndFlushSomeSSTables(cfs, 4, 15);
        TimeWindowCompactionStrategy twcs = createQuicklyExpiringTWCS(cfs, true);

        cfs.getLiveSSTables().forEach(twcs::addSSTable);
        twcs.startup();

        // Wait for the archiving time window to be reached
        Thread.sleep(4000);

        AbstractCompactionTask t = twcs.getNextBackgroundTask(FBUtilities.nowInSeconds());

        // It's now past the archive time window, so there could be an archiving compaction
        // but we are not expecting anything in archive because normal TWCS compaction gets priority
        // We use max depth of 2 to omit backup/snapshot directories
        assertNotNull(t);
        t.execute(null);
        assertFalse(Files.walk(Paths.get(DatabaseDescriptor.getAllArchiveDataFileLocations()[0]).resolve(KEYSPACE1), 2).anyMatch(d -> d.toString().contains("Data.db")));

        cfs.getLiveSSTables().forEach(twcs::addSSTable);
        t = twcs.getNextBackgroundTask(FBUtilities.nowInSeconds());
        t.execute(null);

        // We now expect TWCS to archive the remaining SSTables
        assertTrue(Files.walk(Paths.get(DatabaseDescriptor.getAllArchiveDataFileLocations()[0]).resolve(KEYSPACE1), 2).anyMatch(d -> d.toString().contains("Data.db")));
    }

    //This test doesn't work at the moment because hacking getWriteableLocation will also hack archivingCompactionWriter
    @Test
    @BMRule(name = "Make memtables flush to archive directory instead",
    targetClass = "Directories",
    targetMethod = "getWriteableLocationAsFile",
    action = "$directoryType = Directories$DirectoryType.ARCHIVE")
    public void testCompactionPutsArchivedSSTablesBackIntoArchive() throws InterruptedException, IOException
    {
        final ColumnFamilyStore cfs = prepareCFS();
        TimeWindowCompactionStrategy twcs = createQuicklyExpiringTWCS(cfs, true);
        twcs.startup();

        for (int i = 0; i < 4; i++)
        {
            createAndFlushSomeSSTables(cfs, 1, 60);
        }

        cfs.getLiveSSTables().forEach(twcs::addSSTable);
        twcs.getNextBackgroundTask(FBUtilities.nowInSeconds()).execute(null);

        assertFalse(Files.walk(Paths.get(DatabaseDescriptor.getAllStandardDataFileLocations()[0]).resolve(KEYSPACE1), 2).anyMatch(d -> d.toString().contains("Data.db")));
    }

    @Test
    @BMRules(rules = { @BMRule(name = "Archiving is turned on, but no directories are configured in YAML",
    targetClass = "DatabaseDescriptor",
    targetMethod = "getAllArchiveDataFileLocations",
    action = "return null;") } )
    public void testArchivingIsEnabledInTWCSButNotConfiguredInYAML() throws InterruptedException
    {
        final ColumnFamilyStore cfs = prepareCFS();
        TimeWindowCompactionStrategy twcs = createQuicklyExpiringTWCS(cfs, true);
        twcs.startup();

        createAndFlushSomeSSTables(cfs, 1, 6);
        cfs.getLiveSSTables().forEach(twcs::addSSTable);
        Thread.sleep(4000);

        final AbstractCompactionTask t = twcs.getNextBackgroundTask(FBUtilities.nowInSeconds());
        //Though it's past the archiving window, there are no archive directories so there is nothing to archive and
        //nothing to compact. So we expect t to be null.
        assertNull(t);
    }

    @Test
    public void testArchivingIsDisabledAndArchivingDirConfigured() throws InterruptedException
    {
        final ColumnFamilyStore cfs = prepareCFS();
        TimeWindowCompactionStrategy twcs = createQuicklyExpiringTWCS(cfs, false);
        twcs.startup();

        createAndFlushSomeSSTables(cfs, 1, 6);
        cfs.getLiveSSTables().forEach(twcs::addSSTable);
        Thread.sleep(4000);

        final AbstractCompactionTask t = twcs.getNextBackgroundTask(FBUtilities.nowInSeconds());
        //Archiving isn't actually enabled in the options, so nothing happens
        assertNull(t);
    }

    private void createAndFlushSomeSSTables(ColumnFamilyStore cfs, int numberOfSSTables, int ttl) {
        final ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        for (int i = 0; i < numberOfSSTables; i++)
        {
            DecoratedKey key = Util.dk(String.valueOf("expired"));
            new RowUpdateBuilder(cfs.metadata(), System.currentTimeMillis(), ttl, key.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();

            cfs.forceBlockingFlush();
        }
    }

    private TimeWindowCompactionStrategy createQuicklyExpiringTWCS(ColumnFamilyStore cfs, boolean archivingEnabled) {
        Map<String, String> options = new HashMap<>();

        options.put(COMPACTION_WINDOW_SIZE_KEY, "1");
        options.put(COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        options.put(TIMESTAMP_RESOLUTION_KEY, "MILLISECONDS");
        options.put(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY, "0");
        options.put(CompactionParams.Option.MIN_THRESHOLD.toString(), "2");

        if (archivingEnabled) {
            options.put(ARCHIVE_SSTABLES_SIZE_KEY, "3");
            options.put(ARCHIVE_SSTABLES_UNIT_KEY, "SECONDS");
        }

        return new TimeWindowCompactionStrategy(cfs, options);
    }
}
