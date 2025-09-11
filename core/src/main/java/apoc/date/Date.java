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
package apoc.date;

import static apoc.date.DateUtils.unit;
import static java.time.temporal.ChronoField.*;

import apoc.util.DateFormatUtil;
import apoc.util.Util;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.time.temporal.TemporalQuery;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.procedure.*;

/**
 * @author tkroman
 * @since 9.04.2016
 */
public class Date {
    public static final String DEFAULT_FORMAT = DateUtils.DEFAULT_FORMAT;
    private static final String UTC_ZONE_ID = "UTC";
    private static final List<TemporalQuery<Consumer<FieldResult>>> DT_FIELDS_SELECTORS = Arrays.asList(
            temporalQuery(YEAR),
            temporalQuery(MONTH_OF_YEAR),
            temporalQuery(DAY_OF_WEEK),
            temporalQuery(DAY_OF_MONTH),
            temporalQuery(HOUR_OF_DAY),
            temporalQuery(MINUTE_OF_HOUR),
            temporalQuery(SECOND_OF_MINUTE),
            (temporal) -> (FieldResult result) -> {
                Optional<ZoneId> zone =
                        Optional.ofNullable(TemporalQueries.zone().queryFrom(temporal));
                zone.ifPresent(zoneId -> {
                    String displayName = zoneId.getDisplayName(TextStyle.SHORT, Locale.ROOT);
                    result.value.put("zoneid", displayName);
                    result.zoneid = displayName;
                });
            });

    @UserFunction("apoc.date.toYears")
    @Description("Converts the given timestamp or the given date into a `FLOAT` representing years.")
    public double toYears(
            @Name(value = "value", description = "The timestamp or datetime string to extract the years from.")
                    Object value,
            @Name(
                            value = "format",
                            defaultValue = DEFAULT_FORMAT,
                            description = "The format the given datetime string is in.")
                    String format) {
        if (value instanceof Number) {
            long time = ((Number) value).longValue();
            return (time / (365d * 24 * 3600 * 1000));
        } else {
            long time = parse(value.toString(), "ms", format, null);
            return 1970d + (time / (365d * 24 * 3600 * 1000));
        }
    }

    @UserFunction("apoc.date.fields")
    @QueryLanguageScope(scope = QueryLanguage.CYPHER_5)
    @Description("Splits the given date into fields returning a `MAP` containing the values of each field.")
    public Map<String, Object> fieldsCypher5(
            final @Name(value = "date", description = "A string representation of a temporal value.") String date,
            final @Name(
                            value = "pattern",
                            defaultValue = DEFAULT_FORMAT,
                            description = "The format the given temporal is formatted as.") String pattern) {
        if (date == null) {
            return Util.map();
        }
        DateTimeFormatter fmt = getSafeDateTimeFormatter(pattern);
        TemporalAccessor temporal = fmt.parse(date);
        FieldResult result = new FieldResult();

        for (final TemporalQuery<Consumer<FieldResult>> query : DT_FIELDS_SELECTORS) {
            query.queryFrom(temporal).accept(result);
        }

        return result.asMap();
    }

    @Deprecated
    @UserFunction(value = "apoc.date.fields", deprecatedBy = "Cypher's temporal pattern constructors.")
    @QueryLanguageScope(scope = QueryLanguage.CYPHER_25)
    @Description("Splits the given date into fields returning a `MAP` containing the values of each field.")
    public Map<String, Object> fields(
            final @Name(value = "date", description = "A string representation of a temporal value.") String date,
            final @Name(
                            value = "pattern",
                            defaultValue = DEFAULT_FORMAT,
                            description = "The format the given temporal is formatted as.") String pattern) {
        if (date == null) {
            return Util.map();
        }
        DateTimeFormatter fmt = getSafeDateTimeFormatter(pattern);
        TemporalAccessor temporal = fmt.parse(date);
        FieldResult result = new FieldResult();

        for (final TemporalQuery<Consumer<FieldResult>> query : DT_FIELDS_SELECTORS) {
            query.queryFrom(temporal).accept(result);
        }

        return result.asMap();
    }

    @UserFunction("apoc.date.field")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns the value of one field from the given date time.")
    public Long fieldCypher5(
            final @Name(value = "time", description = "The timestamp in ms since epoch to return a field from.") Long
                            time,
            @Name(value = "unit", defaultValue = "d", description = "The unit of the field to return the value of.")
                    String unit,
            @Name(value = "timezone", defaultValue = "UTC", description = "The timezone the given timestamp is in.")
                    String timezone) {
        return field(time, unit, timezone);
    }

    @Deprecated
    @UserFunction(name = "apoc.date.field", deprecatedBy = "Cypher's `instance.field` component access.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns the value of one field from the given date time.")
    public Long field(
            final @Name(value = "time", description = "The timestamp in ms since epoch to return a field from.") Long
                            time,
            @Name(value = "unit", defaultValue = "d", description = "The unit of the field to return the value of.")
                    String unit,
            @Name(value = "timezone", defaultValue = "UTC", description = "The timezone the given timestamp is in.")
                    String timezone) {
        return (time == null)
                ? null
                : (long) ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.of(timezone))
                        .get(chronoField(unit));
    }

    @UserFunction("apoc.date.currentTimestamp")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns the current Unix epoch timestamp in milliseconds.")
    public long currentTimestampCypher5() {
        return System.currentTimeMillis();
    }

    @Deprecated
    @UserFunction(name = "apoc.date.currentTimestamp", deprecatedBy = "Cypher's `datetime.realtime().epochMillis`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns the current Unix epoch timestamp in milliseconds.")
    public long currentTimestamp() {
        return System.currentTimeMillis();
    }

    public static class FieldResult {
        public final Map<String, Object> value = new LinkedHashMap<>();
        public long years, months, days, weekdays, hours, minutes, seconds;
        public String zoneid;

        public Map<String, Object> asMap() {
            return value;
        }
    }

    private ChronoField chronoField(String unit) {
        switch (unit.toLowerCase()) {
            case "ms":
            case "milli":
            case "millis":
            case "milliseconds":
                return ChronoField.MILLI_OF_SECOND;
            case "s":
            case "second":
            case "seconds":
                return ChronoField.SECOND_OF_MINUTE;
            case "m":
            case "minute":
            case "minutes":
                return ChronoField.MINUTE_OF_HOUR;
            case "h":
            case "hour":
            case "hours":
                return ChronoField.HOUR_OF_DAY;
            case "d":
            case "day":
            case "days":
                return ChronoField.DAY_OF_MONTH;
            case "w":
            case "weekday":
            case "weekdays":
                return ChronoField.DAY_OF_WEEK;
            case "month":
            case "months":
                return ChronoField.MONTH_OF_YEAR;
            case "year":
            case "years":
                return ChronoField.YEAR;
                //			default: return ChronoField.YEAR;
        }

        throw new IllegalArgumentException("The unit: " + unit + " is not correct");
    }

    @UserFunction("apoc.date.format")
    @QueryLanguageScope(scope = QueryLanguage.CYPHER_5)
    @Description(
            "Returns a `STRING` representation of the time value.\n"
                    + "The time unit (default: ms), date format (default: ISO), and time zone (default: current time zone) can all be changed.")
    public String formatCypher5(
            final @Name(value = "time", description = "The timestamp since epoch to format.") Long time,
            @Name(value = "unit", defaultValue = "ms", description = "The unit of the given timestamp.") String unit,
            @Name(
                            value = "format",
                            defaultValue = DEFAULT_FORMAT,
                            description = "The format to convert the given temporal value to.")
                    String format,
            @Name(value = "timezone", defaultValue = "", description = "The timezone the given timestamp is in.")
                    String timezone) {
        return time == null ? null : parse(unit(unit).toMillis(time), format, timezone);
    }

    @Deprecated
    @UserFunction(value = "apoc.date.format", deprecatedBy = "Cypher's format function; format(input, format)")
    @QueryLanguageScope(scope = QueryLanguage.CYPHER_25)
    @Description(
            "Returns a `STRING` representation of the time value.\n"
                    + "The time unit (default: ms), date format (default: ISO), and time zone (default: current time zone) can all be changed.")
    public String format(
            final @Name(value = "time", description = "The timestamp since epoch to format.") Long time,
            @Name(value = "unit", defaultValue = "ms", description = "The unit of the given timestamp.") String unit,
            @Name(
                            value = "format",
                            defaultValue = DEFAULT_FORMAT,
                            description = "The format to convert the given temporal value to.")
                    String format,
            @Name(value = "timezone", defaultValue = "", description = "The timezone the given timestamp is in.")
                    String timezone) {
        return time == null ? null : parse(unit(unit).toMillis(time), format, timezone);
    }

    @UserFunction("apoc.date.toISO8601")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns a `STRING` representation of a specified time value in the ISO8601 format.")
    public String toISO8601Cypher5(
            final @Name(value = "time", description = "The timestamp since epoch to format.") Long time,
            @Name(value = "unit", defaultValue = "ms", description = "The unit of the given timestamp.") String unit) {
        return time == null ? null : parse(unit(unit).toMillis(time), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", null);
    }

    @Deprecated
    @UserFunction(name = "apoc.date.toISO8601", deprecatedBy = "Cypher's `toString()`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns a `STRING` representation of a specified time value in the ISO8601 format.")
    public String toISO8601(
            final @Name(value = "time", description = "The timestamp since epoch to format.") Long time,
            @Name(value = "unit", defaultValue = "ms", description = "The unit of the given timestamp.") String unit) {
        return time == null ? null : parse(unit(unit).toMillis(time), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", null);
    }

    @UserFunction("apoc.date.fromISO8601")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description(
            "Converts the given date `STRING` (ISO8601) to an `INTEGER` representing the time value in milliseconds.")
    public Long fromISO8601Cypher5(
            final @Name(value = "time", description = "The datetime to convert to ms.") String time) {
        return time == null ? null : Instant.parse(time).toEpochMilli();
    }

    @Deprecated
    @UserFunction(name = "apoc.date.fromISO8601", deprecatedBy = "Cypher's `datetime()`")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description(
            "Converts the given date `STRING` (ISO8601) to an `INTEGER` representing the time value in milliseconds.")
    public Long fromISO8601(final @Name(value = "time", description = "The datetime to convert to ms.") String time) {
        return time == null ? null : Instant.parse(time).toEpochMilli();
    }

    @UserFunction("apoc.date.parse")
    @QueryLanguageScope(scope = QueryLanguage.CYPHER_5)
    @Description("Parses the given date `STRING` from a specified format into the specified time unit.")
    public Long parseCypher5(
            @Name(value = "time", description = "The datetime to convert.") String time,
            @Name(value = "unit", defaultValue = "ms", description = "The conversion unit.") String unit,
            @Name(value = "format", defaultValue = DEFAULT_FORMAT, description = "The format the given datetime is in.")
                    String format,
            final @Name(value = "timezone", defaultValue = "", description = "The timezone the given datetime is in.")
                    String timezone) {
        Long value = StringUtils.isBlank(time) ? null : parseOrThrow(time, getFormat(format, timezone));
        return value == null ? null : unit(unit).convert(value, TimeUnit.MILLISECONDS);
    }

    @Deprecated
    @UserFunction(value = "apoc.date.parse", deprecatedBy = "Cypher's temporal pattern constructors.")
    @QueryLanguageScope(scope = QueryLanguage.CYPHER_25)
    @Description("Parses the given date `STRING` from a specified format into the specified time unit.")
    public Long parse(
            @Name(value = "time", description = "The datetime to convert.") String time,
            @Name(value = "unit", defaultValue = "ms", description = "The conversion unit.") String unit,
            @Name(value = "format", defaultValue = DEFAULT_FORMAT, description = "The format the given datetime is in.")
                    String format,
            final @Name(value = "timezone", defaultValue = "", description = "The timezone the given datetime is in.")
                    String timezone) {
        Long value = StringUtils.isBlank(time) ? null : parseOrThrow(time, getFormat(format, timezone));
        return value == null ? null : unit(unit).convert(value, TimeUnit.MILLISECONDS);
    }

    @UserFunction("apoc.date.systemTimezone")
    @Description("Returns the display name of the system time zone (e.g. Europe/London).")
    public String systemTimezone() {
        return TimeZone.getDefault().getID();
    }

    @UserFunction("apoc.date.convert")
    @Description("Converts the given timestamp from one time unit into a timestamp of a different time unit.")
    public Long convert(
            @Name(value = "time", description = "The timestamp to be converted.") long time,
            @Name(value = "unit", description = "The unit the given timestamp is in.") String unit,
            @Name(value = "toUnit", description = "The unit to convert the given timestamp to.") String toUnit) {
        return unit(toUnit).convert(time, unit(unit));
    }

    @UserFunction("apoc.date.convertFormat")
    @QueryLanguageScope(scope = QueryLanguage.CYPHER_5)
    @Description("Converts a `STRING` of one type of date format into a `STRING` of another type of date format.")
    public String convertFormatCypher5(
            @Name(value = "temporal", description = "A string representation of a temporal value.") String input,
            @Name(value = "currentFormat", description = "The format the given temporal is formatted as.")
                    String currentFormat,
            @Name(
                            value = "convertTo",
                            defaultValue = "yyyy-MM-dd",
                            description = "The format to convert the given temporal value to.")
                    String convertTo) {
        if (input == null || input.isEmpty()) {
            return null;
        }

        DateTimeFormatter currentFormatter = DateFormatUtil.getOrCreate(currentFormat);
        DateTimeFormatter convertToFormatter = DateFormatUtil.getOrCreate(convertTo);

        return convertToFormatter.format(currentFormatter.parse(input));
    }

    @Deprecated
    @UserFunction(
            value = "apoc.date.convertFormat",
            deprecatedBy = "Cypher's temporal pattern constructors and format() function.")
    @QueryLanguageScope(scope = QueryLanguage.CYPHER_25)
    @Description("Converts a `STRING` of one type of date format into a `STRING` of another type of date format.")
    public String convertFormat(
            @Name(value = "temporal", description = "A string representation of a temporal value.") String input,
            @Name(value = "currentFormat", description = "The format the given temporal is formatted as.")
                    String currentFormat,
            @Name(
                            value = "convertTo",
                            defaultValue = "yyyy-MM-dd",
                            description = "The format to convert the given temporal value to.")
                    String convertTo) {
        if (input == null || input.isEmpty()) {
            return null;
        }

        DateTimeFormatter currentFormatter = DateFormatUtil.getOrCreate(currentFormat);
        DateTimeFormatter convertToFormatter = DateFormatUtil.getOrCreate(convertTo);

        return convertToFormatter.format(currentFormatter.parse(input));
    }

    @UserFunction("apoc.date.add")
    @Description("Adds a unit of specified time to the given timestamp.")
    public Long add(
            @Name(value = "time", description = "The timestamp to add time to.") long time,
            @Name(value = "unit", description = "The unit the given timestamp is in.") String unit,
            @Name(value = "addValue", description = "The amount of time to add to the given timestamp.") long addValue,
            @Name(value = "addUnit", description = "The unit the added value is in.") String addUnit) {
        long valueToAdd = unit(unit).convert(addValue, unit(addUnit));
        return time + valueToAdd;
    }

    public String parse(
            final @Name("millis") long millis,
            final @Name(value = "pattern", defaultValue = DEFAULT_FORMAT) String pattern,
            final @Name("timezone") String timezone) {
        return getFormat(pattern, timezone).format(new java.util.Date(millis));
    }

    public static DateFormat getFormat(final String pattern, final String timezone) {
        String actualPattern = getPattern(pattern);
        SimpleDateFormat format = null;
        try {
            format = new SimpleDateFormat(actualPattern);
        } catch (Exception e) {
            throw new IllegalArgumentException("The pattern: " + pattern + " is not correct");
        }
        if (timezone != null && !"".equals(timezone)) {
            format.setTimeZone(TimeZone.getTimeZone(timezone));
        } else if (!(containsTimeZonePattern(actualPattern))) {
            format.setTimeZone(TimeZone.getTimeZone(UTC_ZONE_ID));
        }
        return format;
    }

    // work around https://bugs.openjdk.java.net/browse/JDK-8139107
    private static DateTimeFormatter getSafeDateTimeFormatter(final String pattern) {
        DateTimeFormatter safeFormatter = getDateTimeFormatter(pattern);

        if (Locale.UK.equals(safeFormatter.getLocale())) {
            return safeFormatter.withLocale(Locale.ENGLISH);
        }

        return safeFormatter;
    }

    private static DateTimeFormatter getDateTimeFormatter(final String pattern) {
        String actualPattern = getPattern(pattern);
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern(actualPattern);
        if (containsTimeZonePattern(actualPattern)) {
            return fmt;
        } else {
            return fmt.withZone(ZoneId.of(UTC_ZONE_ID));
        }
    }

    public static Long parseOrThrow(final String date, final DateFormat format) {
        if (date == null) return null;
        try {
            return format.parse(date).getTime();
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static boolean containsTimeZonePattern(final String pattern) {
        return pattern.matches("[XxZzVO]{1,3}"); // doesn't account for strings escaped with "'" (TODO?)
    }

    private static String getPattern(final String pattern) {
        return pattern == null || "".equals(pattern) ? DEFAULT_FORMAT : pattern;
    }

    private static TemporalQuery<Consumer<FieldResult>> temporalQuery(final ChronoField field) {
        return temporal -> result -> {
            if (field.isSupportedBy(temporal)) {
                String key = field.getBaseUnit().toString().toLowerCase();
                long value = field.getFrom(temporal);
                switch (field) {
                    case YEAR:
                        result.years = value;
                        break;
                    case MONTH_OF_YEAR:
                        result.months = value;
                        break;
                    case DAY_OF_WEEK:
                        result.weekdays = value;
                        key = "weekdays";
                        break;
                    case DAY_OF_MONTH:
                        result.days = value;
                        break;
                    case HOUR_OF_DAY:
                        result.hours = value;
                        break;
                    case MINUTE_OF_HOUR:
                        result.minutes = value;
                        break;
                    case SECOND_OF_MINUTE:
                        result.seconds = value;
                        break;
                }
                result.value.put(key, value);
            }
        };
    }
}
