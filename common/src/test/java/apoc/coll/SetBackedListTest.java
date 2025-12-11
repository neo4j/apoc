/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.coll;

import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_SET;
import static java.util.Collections.singleton;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SetBackedListTest {

    @Test
    void testEmptyList() {
        var list = new SetBackedList(EMPTY_SET);
        Assertions.assertEquals(0, list.size());
        Assertions.assertTrue(list.isEmpty());
        Assertions.assertFalse(list.contains(1));
        Assertions.assertFalse(list.iterator().hasNext());
        var it = list.listIterator();
        Assertions.assertFalse(it.hasNext());
        Assertions.assertEquals(-1, it.previousIndex());
        Assertions.assertEquals(0, it.nextIndex());
    }

    @Test
    void testSingleList() {
        var list = new SetBackedList(singleton(1));
        Assertions.assertEquals(1, list.size());
        Assertions.assertFalse(list.isEmpty());
        Assertions.assertTrue(list.contains(1));
        Assertions.assertFalse(list.contains(0));
        Assertions.assertTrue(list.iterator().hasNext());
        Assertions.assertEquals(1, list.iterator().next());
        var it = list.listIterator();
        Assertions.assertTrue(it.hasNext());
        Assertions.assertEquals(-1, it.previousIndex());
        Assertions.assertEquals(0, it.nextIndex());
        Assertions.assertEquals(1, it.next());
        Assertions.assertEquals(0, it.previousIndex());
        Assertions.assertEquals(1, it.nextIndex());
        Assertions.assertTrue(it.hasPrevious());
        Assertions.assertEquals(1, it.previous());
        Assertions.assertEquals(1, it.next());
        Assertions.assertFalse(it.hasNext());
    }

    @Test
    void testDoubleList() {
        var list = new SetBackedList(new LinkedHashSet<>(asList(1, 2)));
        Assertions.assertEquals(2, list.size());
        Assertions.assertFalse(list.isEmpty());
        Assertions.assertTrue(list.contains(1));
        Assertions.assertTrue(list.contains(2));
        Assertions.assertFalse(list.contains(0));
        var it = list.iterator();
        Assertions.assertTrue(it.hasNext());
        Assertions.assertEquals(1, it.next());
        Assertions.assertEquals(2, it.next());
        Assertions.assertFalse(it.hasNext());
        var li = list.listIterator();
        Assertions.assertTrue(li.hasNext());
        Assertions.assertEquals(-1, li.previousIndex());
        Assertions.assertEquals(0, li.nextIndex());
        Assertions.assertEquals(1, li.next());
        Assertions.assertEquals(0, li.previousIndex());
        Assertions.assertEquals(1, li.nextIndex());
        Assertions.assertTrue(li.hasPrevious());
        Assertions.assertEquals(1, li.previous());
        Assertions.assertEquals(1, li.next());
        Assertions.assertTrue(li.hasNext());
        Assertions.assertEquals(0, li.previousIndex());
        Assertions.assertEquals(1, li.nextIndex());
        Assertions.assertEquals(2, li.next());
        Assertions.assertEquals(1, li.previousIndex());
        Assertions.assertEquals(2, li.nextIndex());
        Assertions.assertTrue(li.hasPrevious());
        Assertions.assertFalse(li.hasNext());
        Assertions.assertEquals(2, li.previous());
    }

    @Test
    void testReverse() {
        var set = new LinkedHashSet(asList(1, 2, 3, 4, 5));
        var list = new SetBackedList(set);
        Assertions.assertEquals(asList(1, 2, 3, 4, 5), list);

        var it = list.listIterator();
        while (it.hasNext()) it.next();
        var result = new ArrayList(set.size());
        while (it.hasPrevious()) {
            result.add(it.previous());
        }

        Assertions.assertEquals(asList(5, 4, 3, 2, 1), result);
    }

    @Test
    void testContains() {
        var set = new LinkedHashSet(asList(1, 2, 3, 4, 5));
        var list = new SetBackedList(set);
        Assertions.assertTrue(list.contains(1));
        Assertions.assertTrue(list.contains(3));
        Assertions.assertFalse(list.contains(7));
        Assertions.assertFalse(list.containsAll(asList(1, 2, 8)));
        Assertions.assertTrue(list.containsAll(asList(1, 2, 5)));
    }
}
