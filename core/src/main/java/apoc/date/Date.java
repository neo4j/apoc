package apoc.date;

import apoc.util.DateFormatUtil;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.procedure.*;

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

import static apoc.date.DateUtils.unit;
import static java.time.temporal.ChronoField.*;


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
				Optional<ZoneId> zone = Optional.ofNullable(TemporalQueries.zone().queryFrom(temporal));
				zone.ifPresent(zoneId -> {
					String displayName = zoneId.getDisplayName(TextStyle.SHORT, Locale.ROOT);
					result.value.put("zoneid", displayName);
					result.zoneid = displayName;
				});
			}
	);

	@UserFunction("apoc.date.toYears")
	@Description("Converts the given timestamp or the given date into a floating point representing years.")
	public double toYears(@Name("value") Object value, @Name(value = "format", defaultValue = DEFAULT_FORMAT) String format) {
		if (value instanceof Number) {
			long time = ((Number) value).longValue();
			return (time / (365d*24*3600*1000));
		} else {
			long time = parse(value.toString(),"ms",format,null);
			return 1970d + (time / (365d*24*3600*1000));
		}
	}

	@UserFunction("apoc.date.fields")
	@Description("Splits the given date into fields returning a map containing the values of each field.")
	public Map<String,Object> fields(final @Name("date") String date, final @Name(value = "pattern", defaultValue = DEFAULT_FORMAT) String pattern) {
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
	@Description("Returns the value of one field from the given date time.")
	public Long field(final @Name("time") Long time,  @Name(value = "unit", defaultValue = "d") String unit, @Name(value = "timezone",defaultValue = "UTC") String timezone) {
		return (time == null)
				? null
				: (long) ZonedDateTime
				.ofInstant( Instant.ofEpochMilli( time ), ZoneId.of( timezone ) )
				.get( chronoField( unit ) );
	}

	@UserFunction("apoc.date.currentTimestamp")
	@Description( "Returns the current Unix epoch timestamp in milliseconds.")
	public long currentTimestamp()
	{
		return System.currentTimeMillis();
	}

	public static class FieldResult {
		public final Map<String,Object> value = new LinkedHashMap<>();
		public long years, months, days, weekdays, hours, minutes, seconds;
		public String zoneid;

		public Map<String, Object> asMap() {
			return value;
		}
	}

	private ChronoField chronoField(String unit) {
		switch (unit.toLowerCase()) {
			case "ms": case "milli":  case "millis": case "milliseconds": return ChronoField.MILLI_OF_SECOND;
			case "s":  case "second": case "seconds": return ChronoField.SECOND_OF_MINUTE;
			case "m":  case "minute": case "minutes": return ChronoField.MINUTE_OF_HOUR;
			case "h":  case "hour":   case "hours":   return ChronoField.HOUR_OF_DAY;
			case "d":  case "day":    case "days":    return ChronoField.DAY_OF_MONTH;
			case "w":  case "weekday": case "weekdays": return ChronoField.DAY_OF_WEEK;
			case "month":case "months": return ChronoField.MONTH_OF_YEAR;
			case "year":case "years": return ChronoField.YEAR;
//			default: return ChronoField.YEAR;
		}

		throw new IllegalArgumentException("The unit: "+ unit + " is not correct");
	}

	@UserFunction("apoc.date.format")
	@Description("Returns a string representation of the time value.\n" +
			"The time unit (default: ms), date format (default: ISO), and time zone (default: current time zone) can all be changed.")
	public String format(final @Name("time") Long time, @Name(value = "unit", defaultValue = "ms") String unit, @Name(value = "format",defaultValue = DEFAULT_FORMAT) String format, @Name(value = "timezone",defaultValue = "") String timezone) {
		return time == null ? null : parse(unit(unit).toMillis(time), format, timezone);
	}

	@UserFunction("apoc.date.toISO8601")
	@Description("Returns a string representation of a specified time value in the ISO8601 format.")
	public String toISO8601(final @Name("time") Long time, @Name(value = "unit", defaultValue = "ms") String unit) {
		return time == null ? null : parse(unit(unit).toMillis(time), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", null);
	}

	@UserFunction("apoc.date.fromISO8601")
	@Description("Converts the given date string (ISO8601) to an integer representing the time value in milliseconds.")
	public Long fromISO8601(final @Name("time") String time) {
		return time == null ? null : Instant.parse(time).toEpochMilli();
	}

	@UserFunction("apoc.date.parse")
	@Description("Parses the given date string from a specified format into the specified time unit.")
	public Long parse(@Name("time") String time, @Name(value = "unit", defaultValue = "ms") String unit, @Name(value = "format",defaultValue = DEFAULT_FORMAT) String format, final @Name(value = "timezone", defaultValue = "") String timezone) {
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
	public Long convert(@Name("time") long time, @Name(value = "unit") String unit, @Name(value = "toUnit") String toUnit) {
		return unit(toUnit).convert(time, unit(unit));
	}

	@UserFunction("apoc.date.convertFormat")
	@Description("Converts a string of one type of date format into a string of another type of date format.")
	public String convertFormat( @Name( "temporal" ) String input, @Name( value = "currentFormat" ) String currentFormat, @Name( value = "convertTo" , defaultValue = "yyyy-MM-dd" ) String convertTo )
	{
		if (input == null || input.isEmpty())
		{
			return null;
		}

		DateTimeFormatter currentFormatter = DateFormatUtil.getOrCreate( currentFormat );
		DateTimeFormatter convertToFormatter = DateFormatUtil.getOrCreate( convertTo );

		return convertToFormatter.format(  currentFormatter.parse( input ) );
	}

	@UserFunction("apoc.date.add")
	@Description("Adds a unit of specified time to the given timestamp.")
	public Long add(@Name("time") long time, @Name(value = "unit") String unit, @Name(value = "addValue") long addValue, @Name(value = "addUnit") String addUnit) {
		long valueToAdd = unit(unit).convert(addValue, unit(addUnit));
		return time + valueToAdd;
	}

	public String parse(final @Name("millis") long millis, final @Name(value = "pattern", defaultValue = DEFAULT_FORMAT) String pattern, final @Name("timezone") String timezone) {
		return getFormat(pattern, timezone).format(new java.util.Date(millis));
	}

	public static DateFormat getFormat(final String pattern, final String timezone) {
		String actualPattern = getPattern(pattern);
		SimpleDateFormat format = null;
		try {
			format = new SimpleDateFormat(actualPattern);
		} catch(Exception e){
			throw new IllegalArgumentException("The pattern: "+pattern+" is not correct");
		}
		if (timezone != null && !"".equals(timezone)) {
			format.setTimeZone(TimeZone.getTimeZone(timezone));
		} else if (!(containsTimeZonePattern(actualPattern))) {
			format.setTimeZone(TimeZone.getTimeZone(UTC_ZONE_ID));
		}
		return format;
	}

	//work around https://bugs.openjdk.java.net/browse/JDK-8139107
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

	public  static Long parseOrThrow(final String date, final DateFormat format) {
		if (date == null) return null;
		try {
			return format.parse(date).getTime();
		} catch (ParseException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private static boolean containsTimeZonePattern(final String pattern) {
		return pattern.matches("[XxZzVO]{1,3}");	// doesn't account for strings escaped with "'" (TODO?)
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
					case YEAR:             result.years = value;break;
					case MONTH_OF_YEAR:    result.months = value;break;
					case DAY_OF_WEEK:      result.weekdays = value; key = "weekdays"; break;
					case DAY_OF_MONTH:     result.days = value;break;
					case HOUR_OF_DAY:      result.hours = value;break;
					case MINUTE_OF_HOUR:   result.minutes = value;break;
					case SECOND_OF_MINUTE: result.seconds = value;break;
				}
				result.value.put(key, value);
			}
		};
	}
}
