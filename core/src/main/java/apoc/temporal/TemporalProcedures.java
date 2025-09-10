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

import static apoc.date.Date.*;
import static apoc.util.DateFormatUtil.*;
import static apoc.util.DurationFormatUtil.getDurationFormat;
import static apoc.util.DurationFormatUtil.getOrCreateDurationPattern;

import java.time.*;
import java.time.format.DateTimeFormatter;

import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.storable.DurationValue;

public class TemporalProcedures {
    /**
     * Format a temporal value to a String
     *
     * @param input     Any temporal type
     * @param format    A valid DateTime format pattern (ie yyyy-MM-dd'T'HH:mm:ss.SSSS)
     * @return
     */
    @UserFunction("apoc.temporal.format")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Formats the given temporal value into the given time format.")
    public String formatCypher5(
            @Name(value = "temporal", description = "A temporal value to be formatted.") Object input,
            @Name(
                    value = "format",
                    defaultValue = "yyyy-MM-dd",
                    description = "The format to return the temporal value in.")
            String format) {

        try {
            DateTimeFormatter formatter = getOrCreate(format);

            if (input instanceof LocalDate) {
                return ((LocalDate) input).format(formatter);
            } else if (input instanceof ZonedDateTime) {
                return ((ZonedDateTime) input).format(formatter);
            } else if (input instanceof LocalDateTime) {
                return ((LocalDateTime) input).format(formatter);
            } else if (input instanceof LocalTime) {
                return ((LocalTime) input).format(formatter);
            } else if (input instanceof OffsetTime) {
                return ((OffsetTime) input).format(formatter);
            } else if (input instanceof DurationValue) {
                return formatDuration(input, format);
            }
        } catch (Exception e) {
            throw new RuntimeException("Available formats are:\n" + String.join("\n", getTypes())
                    + "\nSee also: https://www.elastic.co/guide/en/elasticsearch/reference/5.5/mapping-date-format.html#built-in-date-formats "
                    + "and https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html");
        }
        return input.toString();
    }
    /**
     * Format a temporal value to a String
     *
     * @param input     Any temporal type
     * @param format    A valid DateTime format pattern (ie yyyy-MM-dd'T'HH:mm:ss.SSSS)
     * @return
     */
    @Deprecated
    @UserFunction(value = "apoc.temporal.format", deprecatedBy = "Cypher's format function; format(input, format)")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Formats the given temporal value into the given time format.")
    public String format(
            @Name(value = "temporal", description = "A temporal value to be formatted.") Object input,
            @Name(
                            value = "format",
                            defaultValue = "yyyy-MM-dd",
                            description = "The format to return the temporal value in.")
                    String format) {

        try {
            DateTimeFormatter formatter = getOrCreate(format);

            if (input instanceof LocalDate) {
                return ((LocalDate) input).format(formatter);
            } else if (input instanceof ZonedDateTime) {
                return ((ZonedDateTime) input).format(formatter);
            } else if (input instanceof LocalDateTime) {
                return ((LocalDateTime) input).format(formatter);
            } else if (input instanceof LocalTime) {
                return ((LocalTime) input).format(formatter);
            } else if (input instanceof OffsetTime) {
                return ((OffsetTime) input).format(formatter);
            } else if (input instanceof DurationValue) {
                return formatDuration(input, format);
            }
        } catch (Exception e) {
            throw new RuntimeException("Available formats are:\n" + String.join("\n", getTypes())
                    + "\nSee also: https://www.elastic.co/guide/en/elasticsearch/reference/5.5/mapping-date-format.html#built-in-date-formats "
                    + "and https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html");
        }
        return input.toString();
    }

    @UserFunction("apoc.temporal.formatDuration")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Formats the given duration into the given time format.")
    public String formatDurationCypher5(
            @Name(value = "input", description = "The duration value to be formatted into a string.") Object input,
            @Name(value = "format", description = "The format to return the duration in.") String format) {
        DurationValue duration = ((DurationValue) input);

        try {
            String pattern = getOrCreateDurationPattern(format);
            return getDurationFormat(duration, pattern);
        } catch (Exception e) {
            throw new RuntimeException("Available formats are:\n" + String.join("\n", getTypes())
                    + "\nSee also: https://www.elastic.co/guide/en/elasticsearch/reference/5.5/mapping-date-format.html#built-in-date-formats "
                    + "and https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html");
        }
    }

    @Deprecated
    @UserFunction(value = "apoc.temporal.formatDuration", deprecatedBy = "Cypher's format function; format(input, format)")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Formats the given duration into the given time format.")
    public String formatDuration(
            @Name(value = "input", description = "The duration value to be formatted into a string.") Object input,
            @Name(value = "format", description = "The format to return the duration in.") String format) {
        DurationValue duration = ((DurationValue) input);

        try {
            String pattern = getOrCreateDurationPattern(format);
            return getDurationFormat(duration, pattern);
        } catch (Exception e) {
            throw new RuntimeException("Available formats are:\n" + String.join("\n", getTypes())
                    + "\nSee also: https://www.elastic.co/guide/en/elasticsearch/reference/5.5/mapping-date-format.html#built-in-date-formats "
                    + "and https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html");
        }
    }

    @UserFunction("apoc.temporal.toZonedTemporal")
    @Description("Parses the given date `STRING` using the specified format into the given time zone.")
    public ZonedDateTime toZonedTemporal(
            @Name(value = "time", description = "The date string to be parsed.") String time,
            @Name(value = "format", defaultValue = DEFAULT_FORMAT, description = "The format of the given date string.")
                    String format,
            final @Name(value = "timezone", defaultValue = "UTC", description = "The timezone the given string is in.")
                    String timezone) {
        Long value = parseOrThrow(time, getFormat(format, timezone));
        return value == null ? null : Instant.ofEpochMilli(value).atZone(ZoneId.of(timezone));
    }
}
