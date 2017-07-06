package cn.memedai.orientdb.sns.realtime.util

import org.apache.commons.lang.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.text.ParseException
import java.text.SimpleDateFormat

/**
 * Created by hangyu on 2017/6/19.
 */
class DateUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(DateUtils.class);

     static String getStartDatetime(String startDatetime, int i) {
        if (StringUtils.isBlank(startDatetime)) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd")
            Date date = new Date(Calendar.getInstance().getTimeInMillis() - i * 3600 * 24 * 1000)
            return sdf.format(date)
        }
        return startDatetime
    }


     static Date StringToDate(String createDate) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        Date date = null
        try {
            date = sdf.parse(createDate)
        } catch (ParseException e) {
            LOGGER.error("StringToDate has e is {}", e)
        }
        return date
    }

    // date类型转换为String类型
    // formatType格式为yyyy-MM-dd HH:mm:ss//yyyy年MM月dd日 HH时mm分ss秒
    // data Date类型的时间
     static String dateToString(Date data, String formatType) {
        return new SimpleDateFormat(formatType).format(data)
    }

    // long类型转换为String类型
    // currentTime要转换的long类型的时间
    // formatType要转换的string类型的时间格式
     static String longToString(long currentTime, String formatType)
            throws ParseException {
        Date date = longToDate(currentTime, formatType);// long类型转成Date类型
        String strTime = dateToString(date, formatType);// date类型转成String
        return strTime
    }

    // string类型转换为date类型
    // strTime要转换的string类型的时间，formatType要转换的格式yyyy-MM-dd HH:mm:ss//yyyy年MM月dd日
    // HH时mm分ss秒，
    // strTime的时间格式必须要与formatType的时间格式相同
     static Date stringToDate(String strTime, String formatType)
            throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat(formatType)
        Date date = null
        date = formatter.parse(strTime)
        return date
    }

    // long转换为Date类型
    // currentTime要转换的long类型的时间
    // formatType要转换的时间格式yyyy-MM-dd HH:mm:ss//yyyy年MM月dd日 HH时mm分ss秒
     static Date longToDate(long currentTime, String formatType)
            throws ParseException {
        Date dateOld = new Date(currentTime) // 根据long类型的毫秒数生命一个date类型的时间
        String sDateTime = dateToString(dateOld, formatType) // 把date类型的时间转换为string
        Date date = stringToDate(sDateTime, formatType) // 把String类型转换为Date类型
        return date
    }

    // string类型转换为long类型
    // strTime要转换的String类型的时间
    // formatType时间格式
    // strTime的时间格式和formatType的时间格式必须相同
     static long stringToLong(String strTime, String formatType)
            throws ParseException {
        Date date = stringToDate(strTime, formatType) // String类型转成date类型
        if (date == null) {
            return 0
        } else {
            long currentTime = dateToLong(date) // date类型转成long类型
            return currentTime
        }
    }

    // date类型转换为long类型
    // date要转换的date类型的时间
     static long dateToLong(Date date) {
        return date == null ? 0L : date.getTime()
    }
}
