package lgh.springboot.starter.hbase.utils;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author Liuguanghua
 *
 */
public final class Convertor {
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final ThreadLocal<SimpleDateFormat> SIMPLE_DATE_FORMAT = new ThreadLocal<SimpleDateFormat>() {
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat(DATETIME_FORMAT);
        }
    };
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(DATETIME_FORMAT);
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(DATE_FORMAT);

    public static byte[] convertFieldValueToHBaseBytes(Object fieldValue, Class<?> type) {
        byte[] value;
        if (type.equals(LocalDateTime.class)) {
            value = Bytes.toBytes(((LocalDateTime) fieldValue).format(DATE_TIME_FORMATTER));
        } else if (type.equals(LocalDate.class)) {
            value = Bytes.toBytes(((LocalDate) fieldValue).format(DATE_FORMATTER));
        } else if (type.equals(Date.class)) {
            value = Bytes.toBytes((SIMPLE_DATE_FORMAT.get()).format((Date) fieldValue));
        } else if (type.equals(String.class)) {
            value = Bytes.toBytes((String) fieldValue);
        } else if (type.equals(int.class) || type.equals(Integer.class)) {
            value = Bytes.toBytes((int) fieldValue);
        } else if (type.equals(boolean.class) || type.equals(Boolean.class)) {
            value = Bytes.toBytes((boolean) fieldValue);
        } else if (type.equals(long.class) || type.equals(Long.class)) {
            value = Bytes.toBytes((long) fieldValue);
        } else if (type.equals(double.class) || type.equals(Double.class)) {
            value = Bytes.toBytes((double) fieldValue);
        } else if (type.equals(Float.class) || type.equals(Float.class)) {
            value = Bytes.toBytes((float) fieldValue);
        } else if (type.equals(short.class) || type.equals(Short.class)) {
            value = Bytes.toBytes((short) fieldValue);
        } else if (type.equals(BigDecimal.class)) {
            value = Bytes.toBytes((BigDecimal) fieldValue);
        } else if (type.equals(byte[].class)) {
            value = (byte[]) fieldValue;
        } else {
            value = Bytes.toBytes(fieldValue.toString());
        }
        return value;
    }

    public static Object convertHBaseBytesToFieldValue(byte[] hbaseValue, Class<?> type) {
        Object value = null;
        if (type.equals(LocalDateTime.class)) {
            value = LocalDateTime.parse(Bytes.toString(hbaseValue), DATE_TIME_FORMATTER);
        } else if (type.equals(LocalDate.class)) {
            value = LocalDate.parse(Bytes.toString(hbaseValue), DATE_FORMATTER);
        } else if (type.equals(Date.class)) {
            try {
                value = SIMPLE_DATE_FORMAT.get().parse(Bytes.toString(hbaseValue));
            } catch (Exception ex) {
                throw new IllegalArgumentException(ex.getMessage(), ex);
            }
        } else if (type.equals(String.class)) {
            value = Bytes.toString(hbaseValue);
        } else if (type.equals(int.class) || type.equals(Integer.class)) {
            value = Bytes.toInt(hbaseValue);
        } else if (type.equals(boolean.class) || type.equals(Boolean.class)) {
            value = Bytes.toBoolean(hbaseValue);
        } else if (type.equals(long.class) || type.equals(Long.class)) {
            value = Bytes.toLong(hbaseValue);
        } else if (type.equals(double.class) || type.equals(Double.class)) {
            value = Bytes.toDouble(hbaseValue);
        } else if (type.equals(Float.class) || type.equals(Float.class)) {
            value = Bytes.toFloat(hbaseValue);
        } else if (type.equals(short.class) || type.equals(Short.class)) {
            value = Bytes.toShort(hbaseValue);
        } else if (type.equals(BigDecimal.class)) {
            value = Bytes.toBigDecimal(hbaseValue);
        } else if (type.equals(byte[].class)) {
            value = hbaseValue;
        }
        return value;
    }

    public static Object convetStringToFieldType(String val, Class<?> type) {
        Object value = null;
        if (type.equals(LocalDateTime.class)) {
            value = LocalDateTime.parse(val, DATE_TIME_FORMATTER);
        } else if (type.equals(LocalDate.class)) {
            value = LocalDate.parse(val, DATE_FORMATTER);
        } else if (type.equals(Date.class)) {
            try {
                value = SIMPLE_DATE_FORMAT.get().parse(val);
            } catch (Exception ex) {
                throw new IllegalArgumentException(ex.getMessage(), ex);
            }
        } else if (type.equals(String.class)) {
            value = val;
        } else if (type.equals(int.class) || type.equals(Integer.class)) {
            value = Integer.parseInt(val);
        } else if (type.equals(boolean.class) || type.equals(Boolean.class)) {
            value = Boolean.parseBoolean(val);
        } else if (type.equals(long.class) || type.equals(Long.class)) {
            value = Long.parseLong(val);
        } else if (type.equals(double.class) || type.equals(Double.class)) {
            value = Double.parseDouble(val);
        } else if (type.equals(Float.class) || type.equals(Float.class)) {
            value = Float.parseFloat(val);
        } else if (type.equals(short.class) || type.equals(Short.class)) {
            value = Short.parseShort(val);
        } else if (type.equals(BigDecimal.class)) {
            value = new BigDecimal(val);
        } else if (type.equals(byte[].class)) {
            value = val.getBytes();
        } else {
            value = val;
        }
        return value;
    }

}
