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

package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.junit.Assert.assertEquals;

public class MemtableTest
{
    @Before
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    /**
local ranges: (10, 100], (120, 200], (300, 400], (600, 800], (900, 910]

disk bounds: 300, 600, 1000
   -> flush: [100, 300], [600], [800, 1000]
     *
     *
     */
    public void testRunnablePerRange()
    {
        List<List<PartitionPosition>> expected = Lists.newArrayList(list(100, 300), list(600), list(800, 1000));

        assertEquals(expected,
                     Memtable.createFlushRunnablePerRangeHelper(localRanges(r(10,100),
                                                                            r(120, 200),
                                                                            r(300, 400),
                                                                            r(600, 800),
                                                                            r(900, 910)),
                                                                boundaries(t(300),
                                                                           t(600),
                                                                           t(1000)),
                                                                t(0).minKeyBound()));
    }

    /**
local ranges: (100,200], (600, 700]
diskbounds: 300, 550, 1000
one range on disk1, no ranges on disk2, one range on disk3
     */
    @Test
    public void testRunnablePerRangeEmpty()
    {
        List<List<PartitionPosition>> expected = Lists.newArrayList(list(300), list(550), list(1000));

        assertEquals(expected,
                     Memtable.createFlushRunnablePerRangeHelper(localRanges(r(100,200),
                                                                            r(600, 700)),
                                                                boundaries(t(300),
                                                                           t(550), // this will be empty
                                                                           t(1000)),
                                                                t(0).minKeyBound()));
    }

    /**
     local ranges: (100,200], (300,500], (600, 700]
     diskbounds: 400, 1000
     0 -> 200 sstable 1 on disk1
     200 -> 400 sstable 2 disk1
     400 -> 500 sstable 3 disk2
     500 -> 1000 sstable 4 disk2
     */
    @Test
    public void testRunnablePerRangeSplit()
    {
        List<List<PartitionPosition>> expected = Lists.newArrayList(list(200, 400), list(500, 1000));

        assertEquals(expected,
                     Memtable.createFlushRunnablePerRangeHelper(localRanges(r(100,200),
                                                                            r(300, 500),
                                                                            r(600, 700)),
                                                                boundaries(t(400),
                                                                           t(1000)),
                                                                t(0).minKeyBound()));
    }


    private List<Range<Token>> localRanges(Range<Token> ... ranges)
    {
        List<Range<Token>> localRanges = new ArrayList<>();
        Collections.addAll(localRanges, ranges);
        return localRanges;
    }

    private List<PartitionPosition> boundaries(Token ... t)
    {
        List<PartitionPosition> boundaries = new ArrayList<>();
        for (Token token : t)
            boundaries.add(token.minKeyBound());
        return boundaries;
    }

    private Range<Token> r(long s, long e)
    {
        return new Range<>(t(s), t(e));
    }

    private Token t(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    private List<PartitionPosition> list(long ... ls)
    {
        List<PartitionPosition> positions = new ArrayList<>();
        for (long l : ls)
            positions.add(t(l).minKeyBound());
        return positions;
    }
}
