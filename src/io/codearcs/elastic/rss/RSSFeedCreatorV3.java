package io.codearcs.elastic.rss;

import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.sun.syndication.feed.synd.SyndContent;
import com.sun.syndication.feed.synd.SyndContentImpl;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndEntryImpl;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.feed.synd.SyndFeedImpl;

import vc.inreach.aws.request.AWSSigner;
import vc.inreach.aws.request.AWSSigningRequestInterceptor;

public class RSSFeedCreatorV3 extends RSSFeedCreator {

	// Kibana URLs
	private static final String KIBANA_INTERNAL_URL = "/kibana-int/dashboard/";
	private static final String KIBANA_DASHBOARD_URL = "/_plugin/kibana3/#/dashboard/elasticsearch/";
	private static final String LOG_URL = "/_plugin/kibana3/#/dashboard/elasticsearch/RSS";

	public JSONObject getDashboard(String host, String encodedDashboard, String dashboard, AWSSigner awsSigner)
			throws RSSException {
		return getRequest(host + KIBANA_INTERNAL_URL + encodedDashboard, dashboard, awsSigner);
	}

	public Dashboard parseDashboard(JSONObject dashboardResponse, String logsHost, String elasticsearchHost,
			AWSSigner awsSigner) throws RSSException {

		try {
			/*
			 * { "_source": { "user": "guest", "group": "guest", "title":
			 * "DashboardTitle", "dashboard": { "title": "Title", "services":
			 * {}}}}
			 */
			if (dashboardResponse.getBoolean("found")) { // output not a
															// dashboard
				JSONObject sourceObject = dashboardResponse.getJSONObject("_source");
				String dashboardString = sourceObject.getString("dashboard");
				JSONObject dashboardObject = new JSONObject(dashboardString);
				JSONObject servicesObject = dashboardObject.getJSONObject("services");

				// Get index pattern for log file naming ie "[index-]YYYY.MM.DD"
				// This is used for POST request like
				// host/index/_search?timeout=3m{
				JSONObject indexObject = dashboardObject.getJSONObject("index");
				String pattern = (String) indexObject.get("pattern");

				// Create Dashboard object to contain dashboard data including
				// filter options
				String title = dashboardObject.getString("title");
				Dashboard dashboard = new Dashboard(title, pattern, logsHost + KIBANA_DASHBOARD_URL + title, 3);

				// Get list of filters
				JSONObject filterObject = servicesObject.getJSONObject("filter");
				JSONArray filterCount = filterObject.getJSONArray("ids");
				JSONObject filterListObject = filterObject.getJSONObject("list");

				for (int i = 0; i < filterCount.length(); i++) {
					int id = filterCount.getInt(i);
					JSONObject filter = filterListObject.getJSONObject(Integer.toString(id));
					if (!"time".equals(filter.getString("type")) && filter.getBoolean("active")) {
						// add filters to dashboard object
						String mandate = filter.getString("mandate");
						boolean negate = mandate.equals("mustNot");
						if (filter.has("value")) {
							dashboard.addOption(filter.getString("type"), filter.getString("field"),
									filter.getString("value"), negate);
						}
						if (filter.has("query")) {
							dashboard.addOption(filter.getString("type"), filter.getString("field"),
									filter.getString("query"), negate);
						}

					}
				}

				// Get list of lucene queries
				JSONObject queryObject = servicesObject.getJSONObject("query");
				JSONArray queryCount = queryObject.getJSONArray("ids");
				JSONObject queryListObject = queryObject.getJSONObject("list");

				for (int i = 0; i < queryCount.length(); i++) {
					int id = queryCount.getInt(i);
					JSONObject query = queryListObject.getJSONObject(Integer.toString(id));
					if (!"time".equals(query.getString("type")) && query.getBoolean("enable")) {
						// add filters to dashboard object
						if (!query.getString("query").isEmpty()) {
							dashboard.addQuery(query.getString("query"));
						}
					}
				}

				return dashboard;
			} else {
				return null;
			}
		} catch (JSONException e) {
			throw new RSSException(
					"Encountered an error parsing dashboard [" + dashboardResponse + "] : " + e.getMessage(), e);
		}

	}

	// return response from post
	public List<Log> doSearch(Dashboard dashboard, String host, String templatePath, AWSSigner awsSigner)
			throws RSSException {
		JSONObject dashboardQuery = createSearchQuery(dashboard, templatePath);

		// take dashboard index and replace date. ASSUME date format is
		// YYYY.MM.DD
		// start with most recent date query logs
		// if less than 50 logs for first date, query previous day until we have
		// a list of 50
		// url host + index + /_search?timeout=3m

		LocalDate dateOnIteration = LocalDate.now();
		List<Log> logs = new ArrayList<>();

		String indexPattern = dashboard.getIndexPattern().replace("[", "").replace("]", "");

		// first request todays logs then minus a day on each iteration
		for (int i = 0; i < NUMBER_OF_DAYS; i++) {

			// get date to replace index pattern date. Assumption is YYYY.MM.DD
			String year = String.valueOf(dateOnIteration.getYear());
			String monthFormatted = String.format("%02d", dateOnIteration.getMonthValue());
			String dayFormatted = String.format("%02d", dateOnIteration.getDayOfMonth());

			// this works for [index-]YYYY.MM.DD but what if pattern is
			// different?
			String index = indexPattern.replace("YYYY", year).replace("MM", monthFormatted).replace("DD", dayFormatted);

			// create url for searching logs

			String logSearchUrl = host + "/" + index + "/_search";

			HttpPost request = new HttpPost(logSearchUrl);
			request.addHeader("content-type", "application/json");

			StringEntity params = new StringEntity(dashboardQuery.toString(), "UTF-8");
			request.setEntity(params);

			try (CloseableHttpClient client = HttpClientBuilder.create()
					.addInterceptorLast(new AWSSigningRequestInterceptor(awsSigner)).build();) {

				HttpResponse response = client.execute(request);

				JSONObject logsResponse = getResponseBody(response);
				JSONObject hits = logsResponse.getJSONObject("hits");
				JSONArray hitsArray = hits.getJSONArray("hits");
				if (hits.getInt("total") != 0) {
					for (int j = 0; j < hitsArray.length(); j++) {
						// if we have enough logs, break out
						if (logs.size() == MAX_LOGS) {
							break;
						}

						// add log to list
						logs.add(createLog((JSONObject) hitsArray.get(j)));
					}
				}

			} catch (IOException | ParseException e) {
				throw new RSSException(
						"Encountered an error performing search for logs on date [" + year + "." + monthFormatted + "."
								+ dayFormatted + "] for dashboard [" + dashboard.getTitle() + "]: " + e.getMessage(),
						e);
			}

			// if we have enough logs, break out
			if (logs.size() == MAX_LOGS) {
				break;
			}

			// Subtract a day
			dateOnIteration = dateOnIteration.minusDays(1);

		}

		return logs;
	}

	private String setFilters(String searchTemplate, Dashboard dashboard) {
		// add filters
		String filterList = "";
		for (String field : dashboard.getFields()) {
			JSONObject terms = new JSONObject(TERMS_TEMPLATE);
			JSONObject termsObject = terms.getJSONObject("terms");
			// need "host.raw": ["build03", "build02"]
			String[] valueArray = new String[dashboard.getValuesForField(field).size()];
			int count = 0;
			for (String value : dashboard.getValuesForField(field)) {
				// create array string of all values for field
				valueArray[count] = value;
				count++;
			}
			// add to terms here
			termsObject.put(field, new JSONArray(Arrays.toString(valueArray)));
			filterList = filterList + terms.toString() + ", ";

		}

		// add filter queries to filter FILTER_QUERY_TEMPLATE
		for (Map.Entry<String, String> query : dashboard.getFilters().getFilterQueries().entrySet()) {
			JSONObject queries = new JSONObject(FILTER_QUERY_TEMPLATE);
			JSONObject fqueryObject = queries.getJSONObject("fquery");
			JSONObject queryObject = fqueryObject.getJSONObject("query");
			JSONObject queryStringObject = queryObject.getJSONObject("query_string");
			/*
			 * "fquery": { "query": { "query_string": { "query":
			 * "id=aeaergqe4g3" } }, "_cache": true }
			 */
			queryStringObject.put("query", query.getValue() + ":(" + query.getKey() + ")");
			filterList = filterList + queries + ", ";
		}
		filterList = filterList.substring(0, filterList.length() - 2);
		searchTemplate = searchTemplate.replace(TERMS, filterList);

		filterList = "";
		// add filter queries to filter FILTER_QUERY_TEMPLATE
		for (Map.Entry<String, String> query : dashboard.getFilters().getFilterQueryNots().entrySet()) {
			JSONObject queries = new JSONObject(FILTER_QUERY_TEMPLATE);
			JSONObject fqueryObject = queries.getJSONObject("fquery");
			JSONObject queryObject = fqueryObject.getJSONObject("query");
			JSONObject queryStringObject = queryObject.getJSONObject("query_string");

			queryStringObject.put("query", query.getValue() + ":(" + query.getKey() + ")");
			filterList = filterList + queries + ", ";
		}

		// remove trailing comma
		if (!filterList.isEmpty()) {
			filterList = filterList.substring(0, filterList.length() - 2);
		}

		// replace @@terms@@
		return searchTemplate.replace(TERMS_NOT, filterList);
	}

	private String setQueries(String searchTemplate, Dashboard dashboard) {
		JSONObject queryTemplate;
		JSONObject queryObject;
		String queryList = "";
		if (!dashboard.getQueries().isEmpty()) {
			for (String query : dashboard.getQueries()) {
				queryTemplate = new JSONObject(QUERY_TEMPLATE);
				queryObject = queryTemplate.getJSONObject("query_string");
				/*
				 * "should": [{ "query_string": { "query": "host.raw = build03"
				 * } }, { "query_string": { "query": "host.raw = build02" } }]
				 */
				// add to queries here
				queryObject.put("query", query);
				queryList = queryList + queryTemplate.toString() + ", ";

			}
			queryList = queryList.substring(0, queryList.length() - 2);
			// replace @@queries@@
			return searchTemplate.replace(QUERIES, queryList);
		}
		return searchTemplate;
	}

	// return json object
	private JSONObject createSearchQuery(Dashboard dashboard, String templatePath) throws RSSException {

		// load template
		String searchTemplate = getSearchQueryTemplate(templatePath);

		// add filters to template search json
		if (!dashboard.getFields().isEmpty()) {
			searchTemplate = setFilters(searchTemplate, dashboard);
		} else {
			// remove ,@@terms@@ because it throws an exception if an empty
			// terms object is there
			searchTemplate = searchTemplate.replace("," + TERMS, "");
			searchTemplate = searchTemplate.replace(TERMS_NOT, "");
		}

		// add queries to template search json
		if (!dashboard.getQueries().isEmpty()) {
			searchTemplate = setQueries(searchTemplate, dashboard);
		} else {
			// remove ,@@queries@@ because it throws an exception if an empty
			// terms object is there
			searchTemplate = searchTemplate.replace(QUERIES, "{\"query_string\": {\"query\": \"*\"}}");
		}

		// replace @@to@@ and @@from@@
		searchTemplate = searchTemplate.replace(TO, String.valueOf(System.currentTimeMillis()));
		searchTemplate = searchTemplate.replace(FROM, String.valueOf(System.currentTimeMillis() - FROM_TIME))
				.replace("\\\\\\\"", "\\\"");

		return new JSONObject(searchTemplate);

	}

	// returns RSS feed for logs
	public SyndFeed createRss(List<Log> logsForRSS, Dashboard dashboard, String host) throws RSSException {

		// create feed entry for each log
		List<SyndEntry> entries = new ArrayList<>();
		for (Log log : logsForRSS) {
			SyndEntry entry;

			SyndContent description;

			entry = new SyndEntryImpl();

			// kibana3: host + LOG_URL + dashboardName +
			// ?id=AVcPc6cjo9Gp7GaCqRKA&from=2016-09-12T09:15:12Z&to=2016-09-12T09:15:12Z
			// subtract day from from date and add day to to date
			entry.setLink(host + LOG_URL + "?id=" + log.getId() + "&from=" + log.getFromDateParam() + "&to="
					+ log.getToDateParam());

			entry.setTitle(log.getMessage() == null ? log.getLogDate().toString() + " : " + entry.getLink()
					: log.getMessage());
			entry.setPublishedDate(log.getLogDate());
			description = new SyndContentImpl();
			description.setType("text/plain");
			description.setValue(log.getMessage());
			entry.setDescription(description);
			entries.add(entry);
		}

		// create feed
		SyndFeed feed = new SyndFeedImpl();

		feed.setFeedType("rss_2.0");
		feed.setTitle(host + " - " + dashboard.getTitle());
		feed.setLink(dashboard.getLink());
		feed.setDescription("This feed has been created for logs from dashboard: " + dashboard.getTitle());
		feed.setEntries(entries);
		return feed;

	}
}
