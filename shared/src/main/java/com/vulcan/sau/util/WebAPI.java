package com.vulcan.sau.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.gargoylesoftware.htmlunit.AjaxController;
import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.NicelyResynchronizingAjaxController;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

public class WebAPI {
	private static final Logger logger = Logger.getLogger(WebAPI.class);

	private static int INTERVAL_BETWEEN_ACCESS = 5000;

	private static long lastAccessed;

	private WebClient webClient = null;

	public WebAPI() {
		lastAccessed = System.currentTimeMillis();

		webClient = new WebClient(BrowserVersion.FIREFOX_17);
		//webClient = new WebClient(BrowserVersion.CHROME);
		webClient.getOptions().setJavaScriptEnabled(true);
		webClient.getOptions().setRedirectEnabled(true);
		webClient.getCookieManager().setCookiesEnabled(true);
		webClient.getOptions().setUseInsecureSSL(true);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);

		webClient.setAjaxController(new NicelyResynchronizingAjaxController());
		
		/*
		webClient.setAjaxController(new AjaxController(){
			public static final long serialVersionUID = 42L;

			@Override
			public boolean processSynchron(HtmlPage page, WebRequest request, boolean async)
			{
				return true;
			}
		});
		*/
		
		Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(Level.OFF);
		System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
	}

	/* Static Methods */
	public static InputStream getWebConnection(String webPageUrl, String userName, String password, String accept) {
		long now = System.currentTimeMillis();
		long delta = now - lastAccessed;

		try{
			if(delta < INTERVAL_BETWEEN_ACCESS){
				Thread.sleep(INTERVAL_BETWEEN_ACCESS - delta);
			}
			lastAccessed = now;

			logger.info(String.format("Sending request [%s]\n", webPageUrl));

			HttpURLConnection connection = (HttpURLConnection) new URL(webPageUrl).openConnection();
			
			if (accept != null) {
				connection.addRequestProperty("Accept", "application/json");
			}
			
			if (userName != null && password != null) {
				connection.setRequestProperty("Authorization", "Basic " + new String(Base64.encodeBase64((userName + ":" + password).getBytes())));
			}
			
			connection.connect();

			if (connection.getResponseCode() != 200) {
				logger.info(String.format("Response: %s %s\n",
						connection.getResponseCode(), connection.getResponseMessage()));

				return null;
			}

			return connection.getInputStream();
		}
		catch(Exception e){
			logger.fatal(e);
		}

		return null;
	}

	public static InputStream getWebConnection(String webPageUrl) {
		return getWebConnection(webPageUrl, null, null, null);
	}

	public static Document getWebPageAsDocument(String webPageUrl) throws IOException {
		return Jsoup.connect(webPageUrl).userAgent("Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36").timeout(60000).get();
	}

	/* Instance Methods */
	public WebClient getWebClient() {
		return webClient;
	}
	
	public String getWebPageAsXml(String webPageUrl, int timeoutInMs) throws Exception {
		return getWebPageAsXml(webPageUrl, null, null, timeoutInMs);
	}

	public String getWebPageAsXml(String webPageUrl) throws Exception {
		return getWebPageAsXml(webPageUrl, null, null, 5000);
	}

	public String getWebPageAsXml(String webPageUrl, String userName, String password) throws Exception {
		return getWebPageAsXml(webPageUrl, userName, password, 5000); 
	}

	public String getWebPageAsXml(String webPageUrl, String userName, String password, int timeoutInMs) throws Exception {
		WebRequest request = new WebRequest(new URL(webPageUrl));

		HashMap<String, String> rHeader = new HashMap<String, String>();
		rHeader.put("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
		rHeader.put("Cache-Control", "max-age=0");
		rHeader.put("Connection", "keep-alive");
		rHeader.put("Accept-Encoding", "gzip,deflate,sdch");
		rHeader.put("Accept-Language", "en-US,en;q=0.8,vi;q=0.6");
		request.setAdditionalHeaders(rHeader);

		HtmlPage page = webClient.getPage(request);

		webClient.waitForBackgroundJavaScript(timeoutInMs);

		synchronized (page) 
		{
			page.wait(2000);
		}

		webClient.getAjaxController().processSynchron(page, request, false);

		String pageAsXml = page.asXml();
		webClient.closeAllWindows();
		page.cleanUp();

		return pageAsXml;
	}

	public String getWebPageAsXml(String webPageUrl, String userName, String password, int timeoutInMs, int waitTimeInMs) throws Exception {
		WebRequest request = new WebRequest(new URL(webPageUrl));

		HtmlPage page = webClient.getPage(request);

		webClient.waitForBackgroundJavaScript(timeoutInMs);

		synchronized (page) 
		{
			page.wait(waitTimeInMs);
		}

		webClient.getAjaxController().processSynchron(page, request, false);

		String pageAsXml = page.asXml();
		webClient.closeAllWindows();
		page.cleanUp();

		return pageAsXml;
	}

	/*
	//Get Page
	HtmlPage page1 = webClient.getPage("https://login-url/");

	//Wait for background Javascript
	webClient.waitForBackgroundJavaScript(10000);

	//Get first form on page
	HtmlForm form = page1.getForms().get(0);

	//Get login input fields using input field name
	HtmlTextInput userName = form.getInputByName("UserName");
	HtmlPasswordInput password = form.getInputByName("Password");

	//Set input values
	userName.setValueAttribute("MyUserName"); 
	password.setValueAttribute("MyPassword"); 

	//Find the first button in form using name, id or xpath
	HtmlElement button = (HtmlElement) form.getFirstByXPath("//button");

	//Post by clicking the button and cast the result, login arrival url, to a new page and repeat what you did with page1 or something else :) 
	HtmlPage page2 = (HtmlPage) button.click(); 

	//Profit
	System.out.println(page2.asXml()); 
	 */
}
