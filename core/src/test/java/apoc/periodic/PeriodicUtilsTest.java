package apoc.periodic;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.List;

import static apoc.periodic.PeriodicUtils.prepareInnerStatement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        Pair<String, Boolean> prepared = prepareInnerStatement("UNWIND $_batch AS p SET p:Actor", batchMode, List.of("p"), "_batch");
        assertTrue(prepared.getRight());
        assertEquals("UNWIND $_batch AS p SET p:Actor", prepared.getLeft());
    }

    @Test
    public void dontUnwindAWithOfColumnNames() {
        BatchMode batchMode = BatchMode.fromIterateList(true);
        Pair<String, Boolean> prepared = prepareInnerStatement("WITH $p as p SET p.lastname=p.name REMOVE p.name", batchMode, List.of("p"), "_batch");
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
        Pair<String, Boolean> prepared = prepareInnerStatement("SET n:Actor, p:Person", batchMode, List.of("n", "p"), "_batch");
        assertFalse(prepared.getRight());
        assertEquals(" WITH $n AS n,$p AS p SET n:Actor, p:Person", prepared.getLeft());
    }

    @Test
    public void dontAddAnExtraWithIfParametersNamedAfterColumnsExistInStatement() {
        BatchMode batchMode = BatchMode.fromIterateList(false);
        Pair<String, Boolean> prepared = prepareInnerStatement("WITH $n AS x SET x:Actor", batchMode, List.of("n"), "_batch");
        assertFalse(prepared.getRight());
        assertEquals("WITH $n AS x SET x:Actor", prepared.getLeft());
    }

    @Test
    public void passThrough$_batchWithNoUnwind() {
        BatchMode batchMode = BatchMode.BATCH_SINGLE;
        Pair<String, Boolean> prepared = prepareInnerStatement("UNWIND $_batch AS batch WITH batch.x AS x SET x:Actor", batchMode, List.of("x"), "_batch");
        assertTrue(prepared.getRight());
        assertEquals("UNWIND $_batch AS batch WITH batch.x AS x SET x:Actor", prepared.getLeft());
    }

}