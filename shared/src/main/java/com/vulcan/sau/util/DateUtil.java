package com.vulcan.sau.util;

import java.util.ArrayList;

public class DateUtil 
{
	/* These are the date formats that we care about:
	 * Y-M-D		Y = Year
	 * Y-MO-D		M = Month (number)
	 * Y-D-M		MO= Month (spelled out)
	 * Y-D-MO		D = Day
	 * M-D-Y
	 * M-Y-D		 
	 * MO-D-Y		
	 * MO-Y-D
	 * D-M-Y
	 * D-MO-Y
	 * D-Y-M
	 * D-Y-MO
	 * Y-M
	 * Y-MO
	 */
	
	public static String parseDate (String dateString)
	{
		if(dateString == null) return null;
		try
		{
			String year  = null;
			String month = null;
			String day   = null;
			
			String[] dateComponents = dateString.contains("-") ? dateString.split("-") : dateString.split(" ");
			
			//boolean twoPartDate = dateComponents.length == 2;
			
			year = findYear(dateComponents);
			// If we don't have a year, don't even bother
			if(year == null) return null;
	
			month = findMonth(dateComponents);
			day = findDay(dateComponents);
			
			return year + "-" + month + "-" + day;
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
			return null;
		}
	}
	
	private static String findDay(String[] dateComponents) 
	{
		ArrayList<Integer> potentialDays = new ArrayList<Integer>();
		
		for (String component : dateComponents)
		{
			if(component.length() == 2)
			{
				try
				{
					potentialDays.add(Integer.parseInt(component));
				}
				catch(NumberFormatException ex)
				{
					// well, it's not an int, so it's not a month.
				}
			}
		}
		
		if(potentialDays.size() == 1)
		{
			return potentialDays.get(0).toString();
		}
		if(potentialDays.size() == 2)
		{
			if(potentialDays.get(0) > 12) return potentialDays.get(0).toString();
			if(potentialDays.get(1) > 12) return potentialDays.get(1).toString();
			return potentialDays.get(1).toString();
		}
		return "01";
	}

	private static String findMonth(String[] dateComponents) 
	{
		// If there are 2 potential months, assume the month comes first
		ArrayList<String> potentialMonths = new ArrayList<String>();
		
		for (String component : dateComponents)
		{
			// first look to see if the month is spelled out
			String month = ConvertEnglishMonth(component);
			if(month != null) return month; 
			if(component.length() == 2)
			{
				try
				{
					int potentialMonth = Integer.parseInt(component);
					if(potentialMonth <= 12) potentialMonths.add(component);
				}
				catch(NumberFormatException ex)
				{
					// well, it's not an int, so it's not a month.
				}
			}
		}
		
		return potentialMonths.size() > 0 ? potentialMonths.get(0) : "01";
	}

	private static String ConvertEnglishMonth(String component) {

		String lowerCase = component.toLowerCase();
		
		if(lowerCase.startsWith("ja"))  return "01";
		if(lowerCase.startsWith("fe"))  return "02";
		if(lowerCase.startsWith("mar")) return "03";
		if(lowerCase.startsWith("ap"))  return "04";
		if(lowerCase.startsWith("may")) return "05";
		if(lowerCase.startsWith("jun")) return "06";
		if(lowerCase.startsWith("jul")) return "07";
		if(lowerCase.startsWith("au"))  return "08";
		if(lowerCase.startsWith("se"))  return "09";
		if(lowerCase.startsWith("oc"))  return "10";
		if(lowerCase.startsWith("no"))  return "11";
		if(lowerCase.startsWith("de"))  return "12";
		return null;
	}

	private static String findYear(String[] dateComponents) 
	{
		for (String component : dateComponents)
		{
			if(component.length() > 2)
			{
				try
				{
					Integer.parseInt(component);
					return component;
				}
				catch(NumberFormatException ex)
				{
					// well, it's not an int, so it's not a year.
				}
			}
		}
		return null;
	}
	
	// assume date format of yyyy-MM-dd
	public static int compareDateStrings(String date1, String date2)
	{
		String[] dateOneParts = date1.split("-");
		String[] dateTwoParts = date2.split("-");
		
		if(!dateOneParts[0].equals(dateTwoParts[0]))
		{
			return Integer.parseInt(dateOneParts[0]) > Integer.parseInt(dateTwoParts[0]) ? 1 : -1;
		}
		
		if(!dateOneParts[1].equals(dateTwoParts[1]))
		{
			return Integer.parseInt(dateOneParts[1]) > Integer.parseInt(dateTwoParts[1]) ? 1 : -1;
		}
		
		if(!dateOneParts[2].equals(dateTwoParts[2]))
		{
			return Integer.parseInt(dateOneParts[2]) > Integer.parseInt(dateTwoParts[2]) ? 1 : -1;
		}
		
		return 0;
	}
}
