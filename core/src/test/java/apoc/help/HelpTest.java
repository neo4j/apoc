package apoc.help;

import apoc.bitwise.BitwiseOperations;
import apoc.coll.Coll;
import apoc.diff.Diff;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 06.11.16
 */
public class HelpTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() {
        TestUtil.registerProcedure(db, Help.class, BitwiseOperations.class, Coll.class, Diff.class);
    }

    @Test
    public void info() {
        TestUtil.testCall(db,"CALL apoc.help($text)",map("text","bitwise"), (row) -> {
            assertEquals("function",row.get("type"));
            assertEquals("apoc.bitwise.op",row.get("name"));
            assertEquals(true, ((String) row.get("text")).contains("bitwise operation"));
        });
        TestUtil.testCall(db,"CALL apoc.help($text)",map("text","operation+"), (row) -> assertEquals("apoc.bitwise.op",row.get("name")));
        TestUtil.testCall(db,"CALL apoc.help($text)",map("text","toSet"), (row) -> {
            assertEquals("function",row.get("type"));
            assertEquals("apoc.coll.toSet",row.get("name"));
            assertEquals(true, ((String) row.get("text")).contains("unique list"));
        });
        TestUtil.testCall(db,"CALL apoc.help($text)",map("text","diff.nodes"), (row) -> {
            assertEquals("function",row.get("type"));
            assertEquals("apoc.diff.nodes",row.get("name"));
            assertEquals(true, ((String) row.get("text")).contains("Returns a list"));
        });
    }

    @Test
    public void indicateCore() {
        TestUtil.testCall(db,"CALL apoc.help($text)",map("text","coll.zipToRows"), (row) -> {
            assertEquals(true, row.get("core"));
        });
    }

}
