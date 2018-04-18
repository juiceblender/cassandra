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
package org.apache.cassandra.tools.nodetool;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;

@Command(name = "describecompactionstrategy", description = "Describe current compaction strategy")
public class DescribeCompactionStrategy extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

    @Option(title = "Describe individual rangeIndex", name= { "--rangeindex", "-r"})
    private Integer rangeIndex;
    @Override
    public void execute(NodeProbe probe)
    {
        List<String> keyspaces = parseOptionalKeyspace(args, probe);
        String[] tableNames = parseOptionalTables(args);
        Iterator<Pair<String, ColumnFamilyStoreMBean>> cfsIterator = new KeyspaceTableFilteringIterator(keyspaces, tableNames, probe.getColumnFamilyStoreMBeanProxies());
        while (cfsIterator.hasNext())
        {
            Pair<String, ColumnFamilyStoreMBean> cfsmbean = cfsIterator.next();
            System.out.printf("%n%s %s.%s %s%n", StringUtils.repeat("-", 50), cfsmbean.left, cfsmbean.right.getTableName(), StringUtils.repeat("-", 50));
            if (rangeIndex == null)
                printDescription(System.out, cfsmbean.right.getCompactionStrategyInfo());
            else
                printDetailedDescription(System.out, cfsmbean.right.getCompactionStrategyInfo(), rangeIndex);
        }
    }

    private void printDetailedDescription(PrintStream out, List<Map<String, Object>> compactionStrategyInfo, int rangeIndex)
    {
        for (Map<String, Object> detailedDescription : compactionStrategyInfo)
        {
            boolean repaired = (boolean) detailedDescription.get("repaired");
            String location = detailedDescription.get("location").toString();
            if (detailedDescription.containsKey("innerStrategies"))
            {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> innerStrategies = (List<Map<String, Object>>) detailedDescription.get("innerStrategies");
                Map<String, Object> innerStrategy = innerStrategies.get(rangeIndex);
                out.printf("* Location=%s (%s) boundaries=%s%n", location, repaired ? "repaired" : "unrepaired", innerStrategy.get("boundaries"));
                if (innerStrategy.containsKey("sstables"))
                {
                    @SuppressWarnings("unchecked")
                    List<String> sstables = (List<String>) innerStrategy.get("sstables");
                    sstables.forEach(out::println);
                }
                else
                    out.println("No sstables");
            }
        }
    }

    private void printDescription(PrintStream out, List<Map<String, Object>> descriptions)
    {
        for (Map<String, Object> description : descriptions)
        {
            out.printf("Strategy=%s, for %d %s sstables, boundary tokens=%s, location=%s%n", description.get("class"), (int)description.get("sstablecount"), ((boolean)description.get("repaired")) ? "repaired" : "unrepaired", description.get("boundaries"), description.get("location"));
            if (description.containsKey("innerStrategies"))
            {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> innerStrategies = (List<Map<String, Object>>) description.get("innerStrategies");
                if (innerStrategies.size() > 0)
                {
                    Optional<Map<String, Object>> globalInnerDescription = innerStrategies.stream().filter(s -> s != null).findFirst();
                    out.printf("Inner strategy: %s (%d instances, %d total sstables)%n", globalInnerDescription.isPresent() ? globalInnerDescription.get().get("class") : "unknown",
                                                                                         innerStrategies.size(),
                                                                                         innerStrategies.stream().collect(Collectors.summingInt(inner -> inner == null || !inner.containsKey("sstablecount") ? 0 : (int) inner.get("sstablecount"))));
                    out.printf("  sstable counts: %n");
                    out.printf("%10s", "");
                    int wrapCount = 30 > innerStrategies.size() ? innerStrategies.size() : 30;
                    for (int i = 0; i < wrapCount; i++)
                        out.printf("%3d", i);
                    out.printf("%n          %s", StringUtils.repeat("-", wrapCount*3));
                    int i = 0;
                    for (Map<String, Object> innerStrategy : innerStrategies)
                    {
                        if (i % wrapCount == 0)
                            out.printf("%n%3d..%3d |", i, i + wrapCount - 1> innerStrategies.size() ? innerStrategies.size() : i + wrapCount - 1);
                        out.printf("%3d", innerStrategy == null || !innerStrategy.containsKey("sstablecount") ? 0 : (int) innerStrategy.get("sstablecount"));
                        i++;
                    }
                    out.println();
                }
            }
        }
    }

    private static class KeyspaceTableFilteringIterator extends AbstractIterator<Pair<String, ColumnFamilyStoreMBean>>
    {
        private final Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> iterator;
        private final Set<String> keyspaces;
        private final Set<String> tableNames;

        public KeyspaceTableFilteringIterator(List<String> keyspaces, String[] tableNames, Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> iterator)
        {
            this.iterator = iterator;
            this.keyspaces = Sets.newHashSet(keyspaces);
            this.tableNames = Sets.newHashSet(tableNames);
        }
        @Override
        protected Pair<String, ColumnFamilyStoreMBean> computeNext()
        {
            while (iterator.hasNext())
            {
                Map.Entry<String, ColumnFamilyStoreMBean> entry = iterator.next();
                String keyspace = entry.getKey();
                if ((keyspaces.isEmpty() || keyspaces.contains(keyspace))
                    && (tableNames.isEmpty() || tableNames.contains(entry.getValue().getTableName())))
                    return Pair.create(keyspace, entry.getValue());
            }
            return endOfData();
        }
    }
}
