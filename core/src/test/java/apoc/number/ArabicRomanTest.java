package apoc.number;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ArabicRomanTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();


    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, ArabicRoman.class);
    }

    @Test
    public void testToArabic() {
        testCall(db, "RETURN apoc.number.romanToArabic('MCMXXXII') AS value", row -> assertEquals(1932, row.get("value")));
        testCall(db, "RETURN apoc.number.romanToArabic('C') AS value", row -> assertEquals(100, row.get("value")));
        testCall(db, "RETURN apoc.number.romanToArabic('mmx') AS value", row -> assertEquals(2010, row.get("value")));
        testCall(db, "RETURN apoc.number.romanToArabic('MXXIV') AS value", row -> assertEquals(1024, row.get("value")));
        testCall(db, "RETURN apoc.number.romanToArabic('aaa') AS value", row -> assertEquals(0,  row.get("value")));
        testCall(db, "RETURN apoc.number.romanToArabic('') AS value", row -> assertEquals(0,  row.get("value")));
        testCall(db, "RETURN apoc.number.romanToArabic(null) AS value", row -> assertEquals(0,  row.get("value")));
    }

    @Test
    public void testToRoman() {
        testCall(db, "RETURN apoc.number.arabicToRoman(1932) AS value", row -> assertEquals("MCMXXXII", row.get("value")));
        testCall(db, "RETURN apoc.number.arabicToRoman(100) AS value", row -> assertEquals("C", row.get("value")));
        testCall(db, "RETURN apoc.number.arabicToRoman(2010) AS value", row -> assertEquals("MMX", row.get("value")));
        testCall(db, "RETURN apoc.number.arabicToRoman(1024) AS value", row -> assertEquals("MXXIV", row.get("value")));
        testCall(db, "RETURN apoc.number.arabicToRoman(null) AS value", row -> assertNull(row.get("value")));
    }
}
