package org.cboard.influxdb;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author LiuTQ
 * @date : 2016-12-29
 * 
 */
public class DateUtils {

	private static Logger log = LoggerFactory.getLogger(DateUtils.class);

	public static String DEFAULT_FORMAT = "yyyy-MM-dd";
	
	public static String FORMAT_STR1 = "yyyy.MM.dd";

	private static String timePattern = "HH:mm";

	public static String timePattern2 = "yyyyMMddHHmmss";

	public static String dateTimePattern = "yyyy-MM-dd HH:mm:ss";
	
	public static String UTC = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
	
	public static void main(String[] a) {
		System.out.println(parseUTC("2018-03-19T07:33:20.905Z"));
	}
	/**
	 *  转换为：yyyy-MM-dd HH:mm:ss
	 * @param time
	 * @return
	 */
	public static String parseUTC2Str(String time,String format) {
		Date d = parseUTC(time);
		return getDateTime(format,d);
	}
	/**
	 * 转换为时间戳
	 * @param time
	 * @return
	 */
	public static long parseToInfluxdbLong(String time) {
		Date d = convertStringToDate("yyyy-MM-dd HH:mm:ss",time);
		return d.getTime()*1000000;
	}
	public static Long parseUTC2Long(String time) {
		return parseUTC(time).getTime();
	}
	/**
	 * UTC 时间转换北京时间
	 * @param time
	 * @return
	 */
	public static Date parseUTC(String time) {
		SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		df2.setTimeZone(TimeZone.getTimeZone("UTC"));
		try {
			return df2.parse(time);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}
	/**
	 * 按时间间隔抽样
	 * 
	 * @param timestampList 
	 * @param betweenSS 间隔时间（秒）
	 * @param maxCount 最大条数（<=0则不限制）
	 * @return
	 */
	public static List<Long> getBySampling(List<Long> timestampList,int betweenSS,int maxCount){
		List<Long> nl = new ArrayList<>();
		for(Long ts : timestampList){
			if(ts == null){
				return null;
			}
			if(nl.isEmpty() || ts.longValue() <= (nl.get(nl.size()-1)-betweenSS)){
				nl.add(ts);
				if(maxCount > 0 && nl.size() >= maxCount){
					break;
				}
			}
		}
		return nl;
	}

	
	public static final String getDate(Date aDate) {
		SimpleDateFormat df = null;
		String returnValue = "";

		if (aDate != null) {
			df = new SimpleDateFormat(DEFAULT_FORMAT);
			returnValue = df.format(aDate);
		}

		return (returnValue);
	}

	public static final String parseToDateTimeStr(Date aDate) {
		SimpleDateFormat df = null;
		String returnValue = "";

		if (aDate != null) {
			df = new SimpleDateFormat(dateTimePattern);
			returnValue = df.format(aDate);
		}

		return (returnValue);
	}
	public static final Long getTimestamp(Date date){
		if(date == null){
			return null;
		}
		return date.getTime()/1000;
	}
	
	public static int daysBetween(Date smdate, Date bdate){
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_FORMAT);
			smdate = sdf.parse(sdf.format(smdate));
			bdate = sdf.parse(sdf.format(bdate));
			Calendar cal = Calendar.getInstance();
			cal.setTime(smdate);
			long time1 = cal.getTimeInMillis(); // smdate
			cal.setTime(bdate);
			long time2 = cal.getTimeInMillis(); // bdate
			long between_days = (time2 - time1) / (1000 * 3600 * 24);

			return Integer.parseInt(String.valueOf(between_days));
		} catch (ParseException e) {
			throw new RuntimeException("daysBetween error"+smdate+";"+bdate,e);
		}
	}
	public static long ssBetweenNow(long time){
		long now = DateUtils.getTimestamp(new Date());
		
		return now - time;
	}
	public static int ssBetween(Date smdate, Date bdate){
		try {
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(smdate);
			long time1 = cal.getTimeInMillis(); // smdate
			cal.setTime(bdate);
			long time2 = cal.getTimeInMillis(); // bdate
			long between_days = (time2 - time1) / 1000;

			return Integer.parseInt(String.valueOf(between_days));
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}
	

	public static int getMonth(Date smdate, Date bdate) throws ParseException {

		int result = 0;

		Calendar c1 = Calendar.getInstance();
		Calendar c2 = Calendar.getInstance();

		c1.setTime(smdate);
		c2.setTime(bdate);

		result = c2.get(Calendar.DAY_OF_MONTH) - c1.get(Calendar.DAY_OF_MONTH);

		return result == 0 ? 1 : Math.abs(result);

	}

	
	public static final Date convertStringToDate(String aMask, String strDate){
		if(StringUtils.isBlank(strDate)){
			return null;
		}
		SimpleDateFormat df = null;
		Date date = null;
		df = new SimpleDateFormat(aMask);

		try {
			date = df.parse(strDate);
		} catch (ParseException pe) {
			pe.printStackTrace();
		}

		return date;
	}

	
	public static String getTimeNow(Date theTime) {
		return getDateTime(timePattern, theTime);
	}

	
	public static Calendar getToday() throws ParseException {
		Date today = new Date();
		SimpleDateFormat df = new SimpleDateFormat(DEFAULT_FORMAT);

		// This seems like quite a hack (date -> string -> date),
		// but it works ;-)
		String todayAsString = df.format(today);
		Calendar cal = new GregorianCalendar();
		cal.setTime(convertStringToDate(todayAsString));

		return cal;
	}

	
	public static final String getDateTime(String aMask, Date aDate) {
		SimpleDateFormat df = null;
		String returnValue = "";

		if (aDate == null) {
			log.error("aDate is null!");
		} else {
			df = new SimpleDateFormat(aMask);
			returnValue = df.format(aDate);
		}

		return (returnValue);
	}

	
	public static final String convertDateToString(Date aDate) {
		return getDateTime(DEFAULT_FORMAT, aDate);
	}


	public static Date convertStringToDate(String strDate) {
		Date aDate = null;

			if (log.isDebugEnabled()) {
				log.debug("converting date with pattern: " + DEFAULT_FORMAT);
			}

			aDate = convertStringToDate(DEFAULT_FORMAT, strDate);
		

		return aDate;
	}

	
	public static Date addDay(Date date, int day) {
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		calendar.add(Calendar.DAY_OF_MONTH, day);
		return calendar.getTime();
	}

	public static Date addHour(Date date, int hour) {
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		calendar.add(Calendar.HOUR_OF_DAY, hour);
		return calendar.getTime();
	}
	public static Date addMinute(Date date, int add) {
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		calendar.add(Calendar.MINUTE, add);
		return calendar.getTime();
	}
	public static Date addSecond(Date date, int add) {
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		calendar.add(Calendar.SECOND, add);
		return calendar.getTime();
	}


	
	public static Date addMonth(Date date, int month) {
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		calendar.add(Calendar.MONTH, month);
		return calendar.getTime();
	}

	public static int getDay(Date d) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(d);
		return calendar.get(Calendar.DATE);
	}

	

	
	public static String getYearFirst(Integer year) {
		return formatDate(getCurrYearFirst(year));
	}

	
	public static String getYearLast(Integer year) {
		return formatDate(getCurrYearLast(year));
	}

	public static Integer getYear() {
		Date date = new Date();
		SimpleDateFormat f = new SimpleDateFormat("yyyy");
		String year = f.format(date);
		return Integer.valueOf(year);

	}

	
	public static String formatDate(Date date) {
		SimpleDateFormat f = new SimpleDateFormat(DEFAULT_FORMAT);
		String sDate = f.format(date);
		return sDate;
	}

	public static String formatDate(String formatPattern, Date date) {
		SimpleDateFormat f = new SimpleDateFormat(formatPattern);
		String sDate = f.format(date);
		return sDate;
	}
	public static final long ss_limit = 10000000000L;
	public static String formatDate(String formatPattern,Long timestamp) {
		if(timestamp==null){
			return null;
		}
		
		if(timestamp <= ss_limit){//10位
			timestamp = timestamp*1000;
		}
		Date d = new Date(timestamp);
		return formatDate(formatPattern,d);
	}
	public static String formatYYYYMMDDHHMMSS(Long timestamp){
		return formatDate("yyyy-MM-dd HH:mm:ss",timestamp);
	}

	
	public static Date getCurrYearFirst(int year) {
		Calendar calendar = Calendar.getInstance();
		calendar.clear();
		calendar.set(Calendar.YEAR, year);
		Date currYearFirst = calendar.getTime();
		return currYearFirst;
	}

	
	public static Date preciseToDay(Date d) {
		String day = getDateTime(DEFAULT_FORMAT, d);//yyyy-MM-dd 格式转换成字符串
		try {
			return convertStringToDate(DEFAULT_FORMAT, day);
		} catch (Exception e) {
			
			return null;
		}
	}
	
	public static Date getTodayZero(Date d) {//当天时间0点
		String day = getDateTime(DEFAULT_FORMAT, d);//yyyy-MM-dd 格式转换成字符串
		try {
			return convertStringToDate(DEFAULT_FORMAT, day);
		} catch (Exception e) {
			
			return null;
		}
	}


	
	public static Date getCurrYearLast(int year) {
		Calendar calendar = Calendar.getInstance();
		calendar.clear();
		calendar.set(Calendar.YEAR, year);
		calendar.roll(Calendar.DAY_OF_YEAR, -1);
		Date currYearLast = calendar.getTime();

		return currYearLast;
	}
	
	public static int getWeek(String dateStr) {
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTime(convertStringToDate(dateStr)); 
		int week = calendar.get(Calendar.DAY_OF_WEEK)-1;
		if(week == 0){
			week = 7;
		}
		return week;
	}
	 
	public static String getMonthFirst() {
		int mondayPlus;
		Calendar cd = Calendar.getInstance();
		
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.set(Calendar.DAY_OF_MONTH, 1);
		Date monday = currentDate.getTime();

		String preMonday = formatDate(monday);

		return preMonday + " 00:00:00";
	}
	
	public static int calContinueDays(List<Date> days){
		int continueDay = 0;
		Date lDay = null;
		for(Date day : days){
			if(lDay != null){
				if(!isContinueDay(day, lDay)){
					return continueDay;
				}
			}
			lDay = day;
			continueDay ++;
		}
		return continueDay;
	}
	public static boolean isContinueDay(Date preDay,Date curDay){
		String d1 = formatDate(addDay(preDay,1));
		String d2 = formatDate(curDay);
		
		return d1.equals(d2);
	}
	public static Date getMinOrderTime(boolean isFront){
		int lazym = isFront?60:30;
		Date d = addMinute(new Date(), lazym);
		
		Calendar c = Calendar.getInstance();
		c.setTime(d);
		
		int minu = c.get(Calendar.MINUTE);
		int add = 60-minu;
		if(add>30){
			add = add-30;
		}
		c.add(Calendar.MINUTE, add);
		c.set(Calendar.SECOND,0);
		c.set(Calendar.MILLISECOND,0);
		
		return c.getTime();
	}
	
}
