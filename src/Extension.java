import java.sql.SQLException;
import java.util.Calendar;

import com.diquest.ir.dbwatcher.DbWatcher;
import com.diquest.ir.dbwatcher.DbWatcherExtension;
import com.diquest.ir.dbwatcher.dbcolumn.DbColumnValue;
import com.diquest.ir.dbwatcher.mapper.FieldMapper;

public class Extension implements DbWatcherExtension {
	public void start(DbWatcher watcher) throws SQLException {
	}

	public void stop() throws SQLException {
	}

	private static int getIndexOf(String name, DbColumnValue[] columnValue) throws SQLException {
		for (int i=0; i < columnValue.length; i++) {
			if (name.equals(columnValue[i].getName()))
				return i;
		}
		throw new SQLException("DBWatcherExtension : " + name + " :alivekim ");
	}
	
	public FieldMapper getMapper(String fieldName, DbColumnValue[] columnValue) throws SQLException {
		if (fieldName.equals("YEAR")) {
			return new YEARMapper(columnValue);
		}else if (fieldName.equals("HALF")) {
			return new MntHalfMapper(columnValue);
		}else if (fieldName.equals("QUARTER")) {
			return new MntQuarterMapper(columnValue);
		}else if (fieldName.equals("MONTH")) {
			return new MntMonthMapper(columnValue);
		}else if (fieldName.equals("WEEK")) {
			return new MntWeekMapper(columnValue);
		}else if (fieldName.equals("DAY")) {
			return new MntDayMapper(columnValue);
		}else if (fieldName.equals("HOUR")) {
			return new MntHourMapper(columnValue);
		}
		
		return null;
	}
	
	private static class YEARMapper extends FieldMapper {
		private final int REGDATE;
		
		public YEARMapper(DbColumnValue[] columnValue) throws SQLException {
			super("YEAR");
			REGDATE = getIndexOf("REGDATE", columnValue);
		}
		public String mapping(DbColumnValue[] value) {
			
			String pubdate = value[REGDATE].getString();
			String mnt_Year = null;

			if(pubdate!=null && pubdate.length()>=4){	
				mnt_Year = pubdate.substring(0, 4);
			}else {
				mnt_Year = "";
			}
			return mnt_Year;
		}
	}
	
	private static class MntHalfMapper extends FieldMapper {
		private final int REGDATE;
		
		public MntHalfMapper(DbColumnValue[] columnValue) throws SQLException {
			super("HALF");
			REGDATE = getIndexOf("REGDATE", columnValue);
		}
		public String mapping(DbColumnValue[] value) {
			
			String pubdate = value[REGDATE].getString();
			String mnt_Year = null;
			String mnt_Half = null;
			
			if(pubdate!=null && pubdate.length()>=6){
				mnt_Year = pubdate.substring(0,4);
			
				int half = 0;
				half = (int)Math.ceil(Integer.parseInt(pubdate.substring(4, 6))/6.0);
				
				mnt_Half = mnt_Year+"0"+half;
			}else{
				mnt_Half = "";
			}
						
			return mnt_Half;
		}
	}
	
	private static class MntQuarterMapper extends FieldMapper {
		private final int REGDATE;
		
		public MntQuarterMapper(DbColumnValue[] columnValue) throws SQLException {
			super("QUARTER");
			REGDATE = getIndexOf("REGDATE", columnValue);
		}
		public String mapping(DbColumnValue[] value) {
			
			String pubdate = value[REGDATE].getString();
					 
			String mnt_Year = null;
			String mnt_Quarter = null;
			
			if(pubdate!=null && pubdate.length()>=6){
				mnt_Year = pubdate.substring(0, 4);
			
				int quarter = 0;
				quarter = (int)Math.ceil(Integer.parseInt(pubdate.substring(4, 6))/3.0);
			
				mnt_Quarter = mnt_Year+"0"+quarter;
			}else{
				mnt_Quarter = "";
			}	
			return mnt_Quarter;
		}
	}
	
	private static class MntMonthMapper extends FieldMapper {
		private final int REGDATE;
		
		public MntMonthMapper(DbColumnValue[] columnValue) throws SQLException {
			super("MONTH");
			REGDATE = getIndexOf("REGDATE", columnValue);
		}
		public String mapping(DbColumnValue[] value) {
			
			String pubdate = value[REGDATE].getString();
			String mnt_Year = null;
			String mnt_Month = null;
		
			if(pubdate!=null && pubdate.length()>=6){	
				mnt_Year = pubdate.substring(0, 4);
				mnt_Month = mnt_Year+pubdate.substring(4, 6);
			}else{
				mnt_Month = "";
			}
			
			return mnt_Month;
		}
	}
	
	private static class MntWeekMapper extends FieldMapper {
		private final int REGDATE;
		
		public MntWeekMapper(DbColumnValue[] columnValue) throws SQLException {
			super("WEEK");
			REGDATE = getIndexOf("REGDATE", columnValue);
		}
		public String mapping(DbColumnValue[] value) {
			
			String pubdate = value[REGDATE].getString();
			String tmp_Year = null;
			String tmp_Month = null;
			String tmp_Day = null;

			String mnt_Year = null;
			String mnt_Month = null;
			String mnt_Week = null;

			if(pubdate!=null && pubdate.length()>=8){
				tmp_Year = pubdate.substring(0, 4);
				tmp_Month = pubdate.substring(4, 6);
				tmp_Day = pubdate.substring(6, 8);
			
				Calendar SCal = Calendar.getInstance();
				SCal.set(Integer.parseInt(tmp_Year), Integer.parseInt(tmp_Month)-1, Integer.parseInt(tmp_Day));
			
				mnt_Year = tmp_Year;
				mnt_Month = tmp_Month;			
		
				int week = 0;
				SCal.setFirstDayOfWeek(java.util.Calendar.SUNDAY);
				SCal.setMinimalDaysInFirstWeek(7);
				week = SCal.get(java.util.Calendar.WEEK_OF_MONTH);
				
				if (week == 0)
				{
					int tmpMonth = Integer.parseInt(tmp_Month)-1;
					if (tmpMonth >= 10) {
						mnt_Month = "" + tmpMonth;
					} else {
						if (tmpMonth == 0) {
							mnt_Month = "12";
							mnt_Year = "" + (Integer.parseInt(mnt_Year)-1);
						}else {
							mnt_Month = "0" + tmpMonth;
						}
					}
					
					SCal.set(Integer.parseInt(tmp_Year), Integer.parseInt(tmp_Month)-2, Integer.parseInt(tmp_Day));
					int endDay = SCal.getActualMaximum(java.util.Calendar.DAY_OF_MONTH);
					SCal.set(Integer.parseInt(tmp_Year), Integer.parseInt(tmp_Month)-2, endDay);
					SCal.setFirstDayOfWeek(java.util.Calendar.SUNDAY);
					SCal.setMinimalDaysInFirstWeek(7);
					week = SCal.get(java.util.Calendar.WEEK_OF_MONTH);
				}
				mnt_Week = mnt_Year+mnt_Month+"0"+week;
			}else{
				mnt_Week = "";
			}		
			return mnt_Week;
		}
	}	
	
	private static class MntDayMapper extends FieldMapper {
		private final int REGDATE;
		
		public MntDayMapper(DbColumnValue[] columnValue) throws SQLException {
			super("DAY");
			REGDATE = getIndexOf("REGDATE", columnValue);
		}
		public String mapping(DbColumnValue[] value) {
			
			String pubdate = value[REGDATE].getString();
			String mnt_Year = null;
			String mnt_Month = null;
			String mnt_Day = null;

			if(pubdate!= null && pubdate.length()>=8){
				mnt_Year = pubdate.substring(0, 4);
				mnt_Month = pubdate.substring(4, 6);
				mnt_Day = mnt_Year+mnt_Month+pubdate.substring(6, 8);
			}else{
				mnt_Day = "";
			}
			return mnt_Day;
		}
	}
	
	private static class MntHourMapper extends FieldMapper {
		private final int REGDATE;
		
		public MntHourMapper(DbColumnValue[] columnValue) throws SQLException {
			super("HOUR");
			REGDATE = getIndexOf("REGDATE", columnValue);
		}
		public String mapping(DbColumnValue[] value) {
			
			String pubdate = value[REGDATE].getString();
			String mnt_Year = null;
			String mnt_Month = null;
			String mnt_Day = null;
			String mnt_Hour = null;
			
			if(pubdate!=null && pubdate.length()>=10){
				mnt_Year = pubdate.substring(0, 4);
				mnt_Month = pubdate.substring(4, 6);
				mnt_Day = pubdate.substring(6, 8);
				mnt_Hour = mnt_Year+mnt_Month+mnt_Day+pubdate.substring(8, 10);
			}else{
				mnt_Hour = "";
			}			
			return mnt_Hour;
		}
	}
}
