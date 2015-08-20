package com.vulcan.sau.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SimilarityUtil {

	public static float similarity(String s1, String s2) {
		

		List<String> tg1 = trigrams(s1);
		List<String> tg2 = trigrams(s2);
		
		System.out.printf("%s\n",tg1 );
		System.out.printf("%s\n", tg2);
		
		Set<String> fullset = new HashSet<String>();
		fullset.addAll(tg1);
		fullset.addAll(tg2);
		int q=0;
		for (String s: tg1) {
			if (tg2.contains(s)) {
				q++;
			}
		}
		
		return (float)q/(float)(fullset.size());
	}

	public static List<String> trigrams(String st) {
		List<String> result = new ArrayList<String>();
		List<String> prefixList = new ArrayList<String>();
		List<String> wordBoundaryList = new ArrayList<String>();
		String lastElement = null;

		for (String s : st.trim().toLowerCase().replaceAll("[^0-9a-z ]", "").split(" ")) {

			if (s.length() < 1) {
				continue;
			}
			else if (s.length() <= 3) {
				result.add(s);
				prefixList.add(s + "!");
				prefixList.add(s.substring(0, 1) + "#");
			} else {
				for (int i = 0; i <= (s.length() - 3); i++) {
					String wip = s.substring(i, i + 3);
					result.add(wip);
				}
				prefixList.add(s.substring(0, 3) + "!");
				prefixList.add(s.substring(0, 1) + "#");
			}

			if (lastElement != null) {
				wordBoundaryList.add(lastElement.substring(0, 1) + " "
						+ s.substring(0, 1));
			}
			lastElement = s;

		}

		result.addAll(prefixList);
		result.addAll(wordBoundaryList);
		return result;
	}

}
