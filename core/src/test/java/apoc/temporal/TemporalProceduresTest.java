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
package apoc.temporal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import apoc.util.TestUtil;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;

@EnterpriseDbmsExtension()
public class TemporalProceduresTest {
    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, TemporalProcedures.class);
    }

    @Test
    void shouldFormatDate() {
        String output = TestUtil.singleResultFirstColumn(
                db,
                "RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), \"yyyy-MM-dd\" ) as output");
        assertEquals("2018-12-10", output);
    }

    @Test
    void shouldFormatDateTime() {
        String output = TestUtil.singleResultFirstColumn(
                db,
                "RETURN apoc.temporal.format( datetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), \"yyyy-MM-dd'T'HH:mm:ss.SSSS\" ) as output");
        assertEquals("2018-12-10T12:34:56.1234", output);
    }

    @Test
    void shouldFormatLocalDateTime() {
        String output = TestUtil.singleResultFirstColumn(
                db,
                "RETURN apoc.temporal.format( localdatetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), \"yyyy-MM-dd'T'HH:mm:ss.SSSS\" ) as output");
        assertEquals("2018-12-10T12:34:56.1234", output);
    }

    @Test
    void shouldFormatTime() {
        String output = TestUtil.singleResultFirstColumn(
                db,
                "RETURN apoc.temporal.format( time( { hour: 12, minute: 34, second: 56, nanosecond: 123456789, timezone: 'GMT' } ), \"HH:mm:ss.SSSSZ\" ) as output");
        assertEquals("12:34:56.1234+0000", output);
    }

    @Test
    void shouldFormatLocalTime() {
        String output = TestUtil.singleResultFirstColumn(
                db,
                "RETURN apoc.temporal.format( localtime( { hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), \"HH:mm:ss.SSSS\" ) as output");
        assertEquals("12:34:56.1234", output);
    }

    @Test
    void shouldFormatDuration() {
        String output = TestUtil.singleResultFirstColumn(
                db, "RETURN apoc.temporal.format( duration('P0M0DT4820.487660000S'), \"HH:mm:ss.SSSS\" ) as output");
        assertEquals("01:20:20.4876", output);
    }

    @Test
    void shouldFormatDurationGreaterThanADay() {
        final String query =
                "WITH duration.between(datetime('2017-06-02T18:40:32.1234560'), datetime('2019-07-13T19:41:33')) AS duration\n"
                        + "RETURN apoc.temporal.formatDuration(duration, $format) AS value";
        String customFormatOne = TestUtil.singleResultFirstColumn(
                db, query, Map.of("format", "yy 'years' MM 'months' www 'weeks' dd 'days' - HH:mm:ss SSSS"));
        assertEquals("02 years 01 months 001 weeks 11 days - 01:01:00 8765", customFormatOne);

        String customFormatTwo =
                TestUtil.singleResultFirstColumn(db, query, Map.of("format", "yyyy 'years' w 'weeks' SSSS"));
        assertEquals("0002 years 1 weeks 8765", customFormatTwo);

        // elastic formats
        String elasticFormatOne = TestUtil.singleResultFirstColumn(db, query, Map.of("format", "date_hour_minute"));
        assertEquals("0002-01-11T01:01", elasticFormatOne);

        String elasticFormatTwo =
                TestUtil.singleResultFirstColumn(db, query, Map.of("format", "date_hour_minute_second_millis"));
        assertEquals("0002-01-11T01:01:00.876", elasticFormatTwo);

        // iso formats
        String isoFormatOne = TestUtil.singleResultFirstColumn(db, query, Map.of("format", "iso_local_date_time"));
        assertEquals("0002-01-11T01:01:00.876544", isoFormatOne);

        String isoFormatTwo = TestUtil.singleResultFirstColumn(db, query, Map.of("format", "iso_ordinal_date"));
        assertEquals("0002-011", isoFormatTwo);
    }

    @Test
    void shouldFormatDurationTemporal() {
        String output = TestUtil.singleResultFirstColumn(
                db, "RETURN apoc.temporal.formatDuration( duration('P0M0DT4820.487660000S'), \"HH:mm:ss\" ) as output");
        assertEquals("01:20:20", output);
    }

    @Test
    void shouldFormatDurationTemporalISO() {
        String output = TestUtil.singleResultFirstColumn(
                db,
                "RETURN apoc.temporal.formatDuration( duration('P0M0DT4820.487660000S'), \"ISO_DATE_TIME\" ) as output");
        assertEquals("0000-00-00T01:20:20.48766", output);
    }

    @Test
    void shouldFormatIsoDate() {
        String output = TestUtil.singleResultFirstColumn(
                db, "RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), 'ISO_DATE' ) as output");
        assertEquals("2018-12-10", output);
    }

    @Test
    void shouldFormatIsoLocalDateTime() {
        String output = TestUtil.singleResultFirstColumn(
                db,
                "RETURN apoc.temporal.format( localdatetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), 'ISO_LOCAL_DATE_TIME' ) as output");
        assertEquals("2018-12-10T12:34:56.123456789", output);
    }

    @Test
    void shouldReturnTheDateWithDefault() {
        String output = TestUtil.singleResultFirstColumn(
                db,
                "RETURN apoc.temporal.format( localdatetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } )) as output");
        assertEquals("2018-12-10", output);
    }

    @Test
    void shouldReturnTheDateWithDefaultElastic() {
        String output = TestUtil.singleResultFirstColumn(
                db,
                "RETURN apoc.temporal.format( localdatetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), 'DATE_HOUR_MINUTE_SECOND_FRACTION') as output");
        assertEquals("2018-12-10T12:34:56.123", output);
    }

    @Test
    void shouldFormatIsoDateWeek() {
        String output = TestUtil.singleResultFirstColumn(
                db, "RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), 'date' ) as output");
        assertEquals("2018-12-10", output);
    }

    @Test
    void shouldFormatIsoYear() {
        String output = TestUtil.singleResultFirstColumn(
                db, "RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), 'date' ) as output");
        assertEquals("2018-12-10", output);
    }

    @Test
    void shouldFormatIsoOrdinalDate() {
        String output = TestUtil.singleResultFirstColumn(
                db,
                "RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), 'ordinal_date' ) as output");
        assertEquals("2018-344", output);
    }

    @Test
    void shouldFormatIsoDateWeekError() {
        var e = assertThrows(
                RuntimeException.class,
                () -> TestUtil.singleResultFirstColumn(
                        db,
                        "RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), 'WRONG_FORMAT' ) as output"));
        assertTrue(e.getMessage().contains("Available formats are:"));
    }

    @Test
    void shouldFormatDurationIsoDateWeekError() {
        var e = assertThrows(
                RuntimeException.class,
                () -> TestUtil.singleResultFirstColumn(
                        db,
                        "RETURN apoc.temporal.formatDuration( date( { year: 2018, month: 12, day: 10 } ), 'wrongDuration' ) as output"));
    }
}
