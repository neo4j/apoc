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
package apoc.periodic;

import static apoc.periodic.PeriodicUtils.prepareInnerStatement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

public class PeriodicUtilsTest {
    @Test
    public void iterateListPrefixActionStatementWithUnwind() {
        BatchMode batchMode = BatchMode.fromIterateList(true);
        Pair<String, Boolean> prepared = prepareInnerStatement("SET n:Actor", batchMode, List.of("n"), "_batch");
        assertTrue(prepared.getRight());
        assertEquals("UNWIND $_batch AS _batch WITH _batch.n AS n  SET n:Actor", prepared.getLeft());
    }

    @Test
    public void dontUnwindAnUnwindOfThe$_batchParameter() {
        BatchMode batchMode = BatchMode.fromIterateList(true);
        Pair<String, Boolean> prepared =
                prepareInnerStatement("UNWIND $_batch AS p SET p:Actor", batchMode, List.of("p"), "_batch");
        assertTrue(prepared.getRight());
        assertEquals("UNWIND $_batch AS p SET p:Actor", prepared.getLeft());
    }

    @Test
    public void dontUnwindAWithOfColumnNames() {
        BatchMode batchMode = BatchMode.fromIterateList(true);
        Pair<String, Boolean> prepared = prepareInnerStatement(
                "WITH $p as p SET p.lastname=p.name REMOVE p.name", batchMode, List.of("p"), "_batch");
        assertFalse(prepared.getRight());
        assertEquals("WITH $p as p SET p.lastname=p.name REMOVE p.name", prepared.getLeft());
    }

    @Test
    public void noIterateListNoPrefixOnActionStatement() {
        BatchMode batchMode = BatchMode.fromIterateList(false);
        Pair<String, Boolean> prepared = prepareInnerStatement("SET n:Actor", batchMode, List.of("n"), "_batch");
        assertFalse(prepared.getRight());
        assertEquals(" WITH $n AS n SET n:Actor", prepared.getLeft());
    }

    @Test
    public void noIterateListNoPrefixOnMultipleActionStatement() {
        BatchMode batchMode = BatchMode.fromIterateList(false);
        Pair<String, Boolean> prepared =
                prepareInnerStatement("SET n:Actor, p:Person", batchMode, List.of("n", "p"), "_batch");
        assertFalse(prepared.getRight());
        assertEquals(" WITH $n AS n,$p AS p SET n:Actor, p:Person", prepared.getLeft());
    }

    @Test
    public void dontAddAnExtraWithIfParametersNamedAfterColumnsExistInStatement() {
        BatchMode batchMode = BatchMode.fromIterateList(false);
        Pair<String, Boolean> prepared =
                prepareInnerStatement("WITH $n AS x SET x:Actor", batchMode, List.of("n"), "_batch");
        assertFalse(prepared.getRight());
        assertEquals("WITH $n AS x SET x:Actor", prepared.getLeft());
    }

    @Test
    public void passThrough$_batchWithNoUnwind() {
        BatchMode batchMode = BatchMode.BATCH_SINGLE;
        Pair<String, Boolean> prepared = prepareInnerStatement(
                "UNWIND $_batch AS batch WITH batch.x AS x SET x:Actor", batchMode, List.of("x"), "_batch");
        assertTrue(prepared.getRight());
        assertEquals("UNWIND $_batch AS batch WITH batch.x AS x SET x:Actor", prepared.getLeft());
    }
}
