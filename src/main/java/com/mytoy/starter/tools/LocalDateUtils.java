package com.mytoy.starter.tools;


import org.apache.commons.lang.StringUtils;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * @author zhangchenglong8
 */
public class LocalDateUtils {

    public static final Pattern patternyyyyMMddHHmmss = Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");

    public static final Pattern patternyyyyMMdd = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");

    public static final Pattern patternHHmmss = Pattern.compile("\\d{2}:\\d{2}:\\d{2}");

    public static final DateTimeFormatter ddMM = DateTimeFormatter.ofPattern("dd/MM");

    public static final DateTimeFormatter ddMMyyyy = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    public static final DateTimeFormatter yyyyMMddHHmmss = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static final DateTimeFormatter yyyyMMdd = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static final DateTimeFormatter HHmmss = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static final LocalTime earlyMorning = LocalTime.parse("00:00:00", HHmmss);

    public static final LocalTime midnight = LocalTime.parse("23:59:59", HHmmss);

    public static final Map<Pattern, DateTimeFormatter> datePatterns = MyMap.<Pattern, DateTimeFormatter>builder().of(patternyyyyMMddHHmmss, yyyyMMddHHmmss).of(patternyyyyMMdd, yyyyMMdd).of(patternHHmmss, HHmmss).build();

    public static final Function<String, TemporalAccessor> adaptParseDateStr = timeStr -> {
        if (StringUtils.isNotBlank(timeStr)) {
            Optional<TemporalAccessor> first = datePatterns.entrySet().stream().filter(vo -> {
                Pattern pattern = vo.getKey();
                if (pattern.matcher(timeStr).find()) return true;
                return false;
            }).map(vo -> {
                DateTimeFormatter formatter = vo.getValue();
                return formatter.parse(timeStr);
            }).findFirst();
            if (first.isPresent()) return first.get();
        }
        return null;
    };

    public static LocalDateTime localDateTime(LocalDate localDate, LocalTime localTime) {
        if (null != localDate && null != localTime) return LocalDateTime.of(localDate, localTime);
        return null;
    }

    public static LocalDate localDate(TemporalAccessor temporalAccessor) {
        if (null != temporalAccessor) return LocalDate.from(temporalAccessor);
        return null;
    }

    public static LocalDateTime localDateTime(TemporalAccessor temporalAccessor) {
        if (null != temporalAccessor) return LocalDateTime.from(temporalAccessor);
        return null;
    }

    public static LocalTime localTime(TemporalAccessor temporalAccessor) {
        if (null != temporalAccessor) return LocalTime.from(temporalAccessor);
        return null;
    }

    public static Long getEpochSecond() {
        return getEpochSecond(Instant.now(Clock.systemDefaultZone()));
    }

    public static Long getEpochSecond(ChronoUnit time, int i) {
        return getEpochSecond(Instant.now(Clock.systemDefaultZone()), time, i);
    }

    public static Long getEpochSecond(Instant instant) {
        return instant.getEpochSecond();
    }

    public static Long getEpochMill(LocalDateTime localDateTime) {
        if (null == localDateTime) return 0l;
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    public static Long getEpochMill() {
        return getEpochMill(LocalDateTime.now(ZoneId.systemDefault()));
    }

    public static Long getEpochSecond(LocalDateTime localDateTime) {
        if (null == localDateTime) return 0l;
        return localDateTime.toInstant(ZoneOffset.of("+8")).getEpochSecond();
    }

    public static LocalDateTime epochSecond2LocalDateTime(Long epochSecond) {
        if (null == epochSecond) return null;
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), ZoneOffset.of("+8"));
    }

    public static Long getEpochSecond(Instant instant, ChronoUnit time, int i) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        LocalDateTime afterPlusTime = localDateTime.plus(i, time);
        Instant newInstant = afterPlusTime.atZone(ZoneId.systemDefault()).toInstant();
        return newInstant.getEpochSecond();
    }

    public static Long getEpochSecond(LocalDate localDate) {
        return localDate.atStartOfDay(ZoneOffset.systemDefault()).toInstant().getEpochSecond();
    }

    public static LocalDateTime epochSecond2LocalDateTime(long epochSecond) {
        Instant instant = Instant.ofEpochMilli(epochSecond);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        return localDateTime;
    }

    public static Duration interval(LocalDateTime start, LocalDateTime end) {
        if (null == start || null == end) return null;
        Duration duration = Duration.between(start, end);
        return duration;
    }

    public static LocalDateTime dated2LocalDateTime(Date date) {
        if (null != date) {
            Instant instant = date.toInstant();
            ZoneId zone = ZoneId.systemDefault();
            LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zone);
            return localDateTime;
        }
        return null;
    }

    public static LocalDateTime str2LocalDateTime(String str, String pattern) {
        if (StringUtils.isNotBlank(str) && StringUtils.isNotBlank(pattern)) {
            LocalDateTime localDateTime = LocalDateTime.parse(str, DateTimeFormatter.ofPattern(pattern));
            return localDateTime;
        }
        return null;
    }

    public static LocalDateTime str2LocalDateTime(String str, DateTimeFormatter pattern) {
        if (StringUtils.isNotBlank(str) && null != pattern) {
            LocalDateTime localDateTime = LocalDateTime.parse(str, pattern);
            return localDateTime;
        }
        return null;
    }

    public static String LocalDateTime2Str(LocalDateTime localDateTime, String pattern) {
        if (null == localDateTime || StringUtils.isBlank(pattern)) return null;
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(localDateTime, ZoneOffset.of("+8"), ZoneId.systemDefault());
        return zonedDateTime.format(DateTimeFormatter.ofPattern(pattern));
    }

    public static Date LocalDateTime2date(LocalDateTime localDateTime) {
        ZoneId zone = ZoneId.systemDefault();
        Instant instant = localDateTime.atZone(zone).toInstant();
        Date date = Date.from(instant);
        return date;
    }

}

