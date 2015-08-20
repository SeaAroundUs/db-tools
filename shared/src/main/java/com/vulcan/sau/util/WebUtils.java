package com.vulcan.sau.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

public class WebUtils {
	public static final String UTF8_CHARSET = "UTF-8";

	public static String percentEncodeRfc3986(String s) {
		String out;
		try {
			out = URLEncoder.encode(s, UTF8_CHARSET).replaceAll("\\+", "%20")
			.replaceAll("\\*", "%2A").replaceAll("%7E", "~");
		} catch (UnsupportedEncodingException e) {
			out = s;
		}
		return out;
	}

	public static int ping(String url, int timeout) {
		int responseCode = -1;
		
		url = url.replaceFirst("https", "http"); // Otherwise an exception may be thrown on invalid SSL certificates.

		try {
			HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
			connection.setConnectTimeout(timeout);
			connection.setReadTimeout(timeout);
			connection.setRequestMethod("GET");
			responseCode = connection.getResponseCode();
			connection.disconnect();
		} catch (IOException exception) {
		}
		
		return(responseCode);
	}
}
