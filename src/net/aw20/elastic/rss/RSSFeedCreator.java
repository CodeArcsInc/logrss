package net.aw20.elastic.rss;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONObject;

import com.google.common.net.UrlEscapers;
import com.sun.syndication.feed.synd.SyndFeed;

import vc.inreach.aws.request.AWSSigner;
import vc.inreach.aws.request.AWSSigningRequestInterceptor;

public abstract class RSSFeedCreator {

	// Templates
	protected static final String TERMS = "@@termsMust@@";
	protected static final String TERMS_NOT = "@@termsMustNot@@";
	protected static final String QUERIES = "@@queries@@";
	protected static final String NOTQUERIES = "@@notqueries@@";
	protected static final String QUERY = "@@query@@";
	protected static final String TO = "@@to@@";
	protected static final String FROM = "@@from@@";

	// Miscellaneous
	protected static final int MAX_LOGS = 50;
	protected static final int NUMBER_OF_DAYS = 4;
	protected static final long FROM_TIME = TimeUnit.DAYS.toMillis(4);

	// ES Templates
	protected static final String TERMS_TEMPLATE = "{\"terms\":{}}";
	protected static final String QUERY_TEMPLATE = "{\"query_string\": {}}";
	protected static final String FILTER_QUERY_TEMPLATE = "{\"fquery\": {\"query\": {\"query_string\": {}},\"_cache\": true}}";

	public JSONObject getRequest(String queryUrl, String dashboard, AWSSigner awsSigner) throws RSSException {

		if (queryUrl != null) {
			HttpGet request = new HttpGet(queryUrl);
			try (CloseableHttpClient client = HttpClientBuilder.create()
					.addInterceptorLast(new AWSSigningRequestInterceptor(awsSigner)).build();) {

				HttpResponse response = client.execute(request);
				if (response.getStatusLine().getStatusCode() >= 300
						&& response.getStatusLine().getStatusCode() != 404) {
					throw new RSSException("Received response code [" + response.getStatusLine().getStatusCode()
							+ "] when attempting to look up dashboard [" + dashboard + "] on elasticsearch - response ["
							+ getResponseBody(response) + "]");
				}
				return getResponseBody(response);

			} catch (IOException e) {
				throw new RSSException(
						"Encountered an error attempting to get dashboard from elasticsearch : " + e.getMessage(), e);
			}
		}
		return null;

	}

	public JSONObject postRequest(String queryUrl, String dashboard, AWSSigner awsSigner, String postBody)
			throws RSSException {
		HttpPost req = new HttpPost(queryUrl);
		req.addHeader("content-type", "application/json");

		StringEntity params = new StringEntity(postBody, "UTF-8");
		req.setEntity(params);
		// HttpGet request = new HttpGet( queryUrl );
		try (CloseableHttpClient client = HttpClientBuilder.create()
				.addInterceptorLast(new AWSSigningRequestInterceptor(awsSigner)).build();) {

			HttpResponse response = client.execute(req);
			if (response.getStatusLine().getStatusCode() >= 300) {
				// for 404, want to return message to say dashboard not found
				throw new RSSException("Received response code [" + response.getStatusLine().getStatusCode()
						+ "] when attempting to look up dashboard [" + dashboard + "] on elasticsearch - response ["
						+ getResponseBody(response) + "]");
			}
			return getResponseBody(response);

		} catch (IOException e) {
			throw new RSSException(
					"Encountered an error attempting to get dashboard from elasticsearch : " + e.getMessage(), e);
		}
	}

	public JSONObject getResponseBody(HttpResponse response) throws IOException {
		// Get the response
		StringBuilder builder = new StringBuilder();

		try (InputStreamReader isr = new InputStreamReader(response.getEntity().getContent());
				BufferedReader rd = new BufferedReader(isr);) {

			String aux = "";
			while ((aux = rd.readLine()) != null) {
				builder.append(aux);
			}

		}

		System.out.println(response);

		return new JSONObject(builder.toString());
	}

	public String getSearchQueryTemplate(String templatePath) throws RSSException {
		char c;
		int i;

		try (FileInputStream fis = new FileInputStream(templatePath);
				InputStreamReader isr = new InputStreamReader(fis);) {
			StringBuilder sb = new StringBuilder();

			// read till the end of the file
			while ((i = isr.read()) != -1) {
				// int to character
				c = (char) i;
				sb.append(c);
			}
			return sb.toString();
		} catch (IOException e) {
			throw new RSSException("Encountered an error attempting to read search query template : " + e.getMessage(),
					e);
		}
	}

	public Log createLog(JSONObject hit) throws ParseException {

		// get log data
		Log log = new Log(hit.getString("_id"), hit.getString("_index"), hit.getString("_type"));

		// get log source data
		JSONObject source = hit.getJSONObject("_source");

		if (source.has("message")) {
			// trim message to 200
			String s = source.getString("message");
			log.setMessage(s.length() > 200 ? s.substring(0, 199) : s);
		}

		if (source.has("@timestamp")) {
			String s = source.getString("@timestamp").replace("T", " ").replace("Z", "");
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			Date date = sdf.parse(s);

			// subtract 1 day
			String dateBefore = sdf.format(new Date(date.getTime() - 1 * 24 * 3600 * 1000l)).replace(" ", "T");
			log.setFromDateParam(dateBefore);

			// add 1 day
			String dateAfter = sdf.format(new Date(date.getTime() + 1 * 24 * 3600 * 1000l)).replace(" ", "T");
			log.setToDateParam(dateAfter);

			log.setLogDate(date);
		}

		return log;
	}

	public String encodeUrl(String url) {
		return UrlEscapers.urlFragmentEscaper().escape(url);
	}

	public abstract JSONObject getDashboard(String host, String encodedDashboard, String dashboardName,
			AWSSigner awsSigner) throws RSSException;

	public abstract Dashboard parseDashboard(JSONObject dashboardResponse, String logsHost, String elasticsearchHost,
			AWSSigner awsSigner) throws RSSException;

	public abstract List<Log> doSearch(Dashboard dashboard, String host, String templatePath, AWSSigner awsSigner)
			throws RSSException;

	public abstract SyndFeed createRss(List<Log> logsForRSS, Dashboard dashboard, String logsHost) throws RSSException;
}
