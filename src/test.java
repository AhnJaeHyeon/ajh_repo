import java.util.Calendar;


public class test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		String pubdate = "20140504";
		String pubdate = "201301204";
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
			
			System.out.println(SCal.getTime().toString());
		
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
				System.out.println(SCal.getTime().toString());
				int endDay = SCal.getActualMaximum(java.util.Calendar.DAY_OF_MONTH);
				SCal.set(Integer.parseInt(tmp_Year), Integer.parseInt(tmp_Month)-2, endDay);
				System.out.println(SCal.getTime().toString());
				SCal.setFirstDayOfWeek(java.util.Calendar.SUNDAY);
				SCal.setMinimalDaysInFirstWeek(7);
				week = SCal.get(java.util.Calendar.WEEK_OF_MONTH);
			}
			mnt_Week = mnt_Year+mnt_Month+"0"+week;
			
		}else{
			mnt_Week = "";
		}		
		
		String gitTest = "master ºê·»Ä¡ Ä¿¹Ô";
		String gitTest = "master ºê·»Ä¡ µÎ¹øÂ° Ä¿¹Ô"; 
		String gitTest = "master ºê·»Ä¡ µÎ¹øÂ° Ä¿¹Ô"; 
		String gitTest = "master ºê·»Ä¡ µÎ¹øÂ° Ä¿¹Ô"; 

		
		String gitrr = "testing";
		String gitrr = "testing";
		
		String gitrr = "¸¶½ºÅÍ Ä¿¹Ô";
		String gitrr = "¸¶½ºÅÍ Ä¿¹Ô";
		String gitrr = "¸¶½ºÅÍ Ä¿¹Ô";
		String gitTest = "Ä¿¹Ô Å×½ºÆ®ÇÒ·Á°í Ãß°¡ÇÔ11";
		
		System.out.println(mnt_Week);
	}

}
