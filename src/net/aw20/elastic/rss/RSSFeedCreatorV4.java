package net.aw20.elastic.rss;

import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

import com.sun.syndication.feed.synd.SyndContent;
import com.sun.syndication.feed.synd.SyndContentImpl;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndEntryImpl;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.feed.synd.SyndFeedImpl;

import vc.inreach.aws.request.AWSSigner;
import vc.inreach.aws.request.AWSSigningRequestInterceptor;

public class RSSFeedCreatorV4 extends RSSFeedCreator {

	// Templates
	private static final String INDEX = "@@index@@";
	private static final String INDEX_DATE = "@@indexDate@@";
	private static final String TYPE = "@@type@@";
	private static final String ID = "@@id@@";
	private static final String DASHBOARD = "@@dashboard@@";
	private static final String PANEL = "@@panel@@";

	// Kibana URLs
	private static final String KIBANA_INTERNAL_URL = "/.kibana-4/_mget?timeout=0&preference=1478625427132";
	private static final String MSEARCH = "/_msearch";
	private static final String LOG_URL = "/_plugin/kibana/#/doc/" + INDEX + "/" + INDEX_DATE + "/" + TYPE + "?id=" + ID
			+ "&_g=()";
	private static final String DASHBOARD_URL = "/_plugin/kibana/#/discover/";

	// Kibana POST bodies
	private static final String DASHBOARD_POST = "{\"docs\":[{\"_type\":\"dashboard\",\"_id\":\"" + DASHBOARD + "\"}]}";
	private static final String PANEL_POST = "{\"docs\":[{\"_type\":\"search\",\"_id\":\"" + PANEL + "\"}]}";
	private static final String EMPTY_QUERY = "{\"query\": {\"match_all\": {}}";

	@Override
	public JSONObject getDashboard(String host, String encodedDashboard, String dashboardName, AWSSigner awsSigner)
			throws RSSException {
		return postRequest(host + KIBANA_INTERNAL_URL, dashboardName, awsSigner,
				DASHBOARD_POST.replace(DASHBOARD, encodedDashboard));
	}

	@Override
	public Dashboard parseDashboard(JSONObject dashboardResponse, String logsHost, String elasticsearchHost,
			AWSSigner awsSigner) throws RSSException {
		// dashboardResponse =
		// {"docs":[{"_index":".kibana-4","_type":"dashboard","_id":"Logs","_version":1,"found":true,"_source":{"title":"Logs","hits":0,"description":"","panelsJSON":"[{\"id\":\"All-Results\",\"type\":\"search\",\"size_x\":3,\"size_y\":2,\"col\":4,\"row\":3},{\"id\":\"All-Docs\",\"type\":\"visualization\",\"size_x\":9,\"size_y\":2,\"col\":4,\"row\":1}]","version":1,"kibanaSavedObjectMeta":{"searchSourceJSON":"{\"filter\":[{\"query\":{\"query_string\":{\"query\":\"*\",\"analyze_wildcard\":true}}}]}"}}}]}
		JSONArray docsObjects = dashboardResponse.getJSONArray("docs");
		// Note: In development, I haven't come across a response
		// where docsObject has more than one array entry, so only
		// getting first entry below
		JSONObject firstSourceObject = docsObjects.getJSONObject(0);
		String dashboardName = firstSourceObject.getString("_id");
		if (firstSourceObject.getBoolean("found")) { // output not a dashboard
			JSONObject sourceObject = firstSourceObject.getJSONObject("_source");
			String panelsObjectString = sourceObject.getString("panelsJSON");

			/*
			 * panelsObjectString = [{ "id": "All-Results", "type": "search",
			 * "size_x": 3, "size_y": 2, "col": 4, "row": 3 }, { "id":
			 * "All-Docs", "type": "visualization", "size_x": 9, "size_y": 2,
			 * "col": 4, "row": 1 }]
			 */

			// get filters
			JSONObject saveObjectMeta = sourceObject.getJSONObject("kibanaSavedObjectMeta");
			String searchSourceJSON = saveObjectMeta.getString("searchSourceJSON");
			JSONObject searchJson = new JSONObject(searchSourceJSON);
			JSONArray filters = searchJson.getJSONArray("filter");

			Map<JSONObject, Boolean> listOfFilters = new HashMap<>();

			int lengthOfFilterArray = filters.length();

			for (int i = 0; i < lengthOfFilterArray; i++) {
				JSONObject filter = filters.getJSONObject(i);
				if (filter.has("meta")) {
					JSONObject meta = filter.getJSONObject("meta");
					if (meta.getBoolean("disabled") == false) {
						JSONObject query = filter.getJSONObject("query");
						// map contains filter and negate flag
						listOfFilters.put(query, meta.getBoolean("negate"));
					}
				}
			}

			// assume only one search per dashboard, find first.
			List<Object> g = new JSONArray(panelsObjectString).toList();
			if (!g.isEmpty()) {
				JSONArray panelsArray = new JSONArray(panelsObjectString);
				String panel = null;
				for (int i = 0; i < panelsArray.length(); i++) {
					JSONObject json = (JSONObject) panelsArray.get(i);
					if (json.get("type").equals("search")) {
						panel = json.getString("id");
						break;
					}
				}

				JSONObject obj = postRequest(elasticsearchHost + KIBANA_INTERNAL_URL, dashboardName, awsSigner,
						PANEL_POST.replace(PANEL, panel));
				// obj =
				// {"docs":[{"found":true,"_index":".kibana-4","_type":"search","_source":{"hits":0,"columns":["_source"],"description":"","sort":["@timestamp","desc"],"title":"DashboardTitle","version":1,"kibanaSavedObjectMeta":{"searchSourceJSON":"{\"index\":\"[index-]YYYY.MM.DD\",\"highlight\":{\"pre_tags\":[\"@kibana-highlighted-field@\"],\"post_tags\":[\"@/kibana-highlighted-field@\"],\"fields\":{\"*\":{}},\"fragment_size\":2147483647},\"filter\":[],\"query\":{\"query_string\":{\"query\":\"*\",\"analyze_wildcard\":true}}}"}},"_id":"DashboardTitle","_version":1}]}
				JSONArray docsObjectsArray = obj.getJSONArray("docs");

				// Note: In development, I haven't come across a response
				// where docsObject has more than one array entry, so only
				// getting first entry below
				JSONObject arrayEntry = docsObjectsArray.getJSONObject(0);
				JSONObject source = arrayEntry.getJSONObject("_source");
				JSONObject panels = source.getJSONObject("kibanaSavedObjectMeta");
				String searchSource = panels.getString("searchSourceJSON");
				JSONObject searchSourceJson = new JSONObject(searchSource);
				// j =
				// {"filter":[],"highlight":{"pre_tags":["@kibana-highlighted-field@"],"post_tags":["@/kibana-highlighted-field@"],"fields":{"*":{}},"fragment_size":2147483647},"query":{"query_string":{"query":"*","analyze_wildcard":true}},"index":"[index-]YYYY.MM.DD"}
				String indexPattern = searchSourceJson.getString("index");

				String title = sourceObject.getString("title");

				// host +
				// /index-2016.11.09/_msearch?timeout=0&preference=1478625427132
				Dashboard d = new Dashboard(title, indexPattern,
						logsHost + DASHBOARD_URL + title.replace(" ", "-") + "?_g=()", 4);

				// add filters from above listOfFilters
				for (Map.Entry<JSONObject, Boolean> entry : listOfFilters.entrySet()) {

					String type = "";
					// {{"match":{"_type":{"query":"aws","type":"phrase"}}}=false,
					JSONObject match = entry.getKey().getJSONObject("match");
					if (match.has("component")) {
						JSONObject _component = match.getJSONObject("component");
						type = _component.getString("type");
					} else {
						JSONObject _type = match.getJSONObject("_type");
						type = _type.getString("type");
					}

					String fullQuery = "{ \"query\" : " + entry.getKey() + "}";

					String query = match.toString();

					d.addOption(type, query, fullQuery, entry.getValue());
				}

				/*
				 * "filter": [{ "meta": { "negate": false, "index":
				 * "[index-]YYYY.MM.DD", "key": "component", "value":
				 * "first_metric", "disabled": false }, "query": { "match": {
				 * "component": { "query": "first_metric", "type": "phrase" } }
				 * } }, { "meta": { "negate": false, "index":
				 * "[index-]YYYY.MM.DD", "key": "metric_name", "value":
				 * "Connections", "disabled": false }, "query": { "match": {
				 * "metric_name": { "query": "Connections", "type": "phrase" } }
				 * } }],
				 */
				JSONArray filter = searchSourceJson.getJSONArray("filter");
				for (int i = 0; i < filter.length(); i++) {
					if (!filter.isNull(i)) {
						JSONObject firstFilterObject1 = filter.getJSONObject(i);
						JSONObject query = firstFilterObject1.getJSONObject("query");
						JSONObject meta = firstFilterObject1.getJSONObject("meta");
						JSONObject q = new JSONObject("{ \"query\" : " + query.toString() + "}");
						// check disabled value in meta
						if (!meta.getBoolean("disabled")) {
							JSONObject match = query.getJSONObject("match");
							String key = meta.getString("key");
							JSONObject queryType = match.getJSONObject(key);
							d.addOption(queryType.getString("type"), queryType.getString("query"), q.toString(),
									meta.getBoolean("negate"));
						}
					}
				}

				if (!searchSourceJson.isNull("query")) {
					JSONObject query = searchSourceJson.getJSONObject("query");
					JSONObject q = new JSONObject("{ \"query\" : " + query.toString() + "}");

					d.addQuery(q.toString());
				}

				return d;
			}
		}
		return null;

	}

	@Override
	public List<Log> doSearch(Dashboard dashboard, String host, String templatePath, AWSSigner awsSigner)
			throws RSSException {
		String searchTemplate = getSearchQueryTemplate(templatePath);
		searchTemplate = setQuery(searchTemplate, dashboard.getQueries());
		searchTemplate = setQueries(searchTemplate, dashboard.getFilters().getFilterQueries(),
				dashboard.getFilters().getFilterQueryNots(), dashboard.getQueries());

		searchTemplate = searchTemplate.replaceAll(TO, String.valueOf(System.currentTimeMillis()));
		searchTemplate = searchTemplate.replaceAll(FROM, String.valueOf(System.currentTimeMillis() - FROM_TIME));

		String indexPattern = dashboard.getIndexPattern().replace("[", "").replace("]", "");
		// need to replace YYYY.MM.DD with actual dates and comma deliminate
		// them
		LocalDate dateOnIteration = LocalDate.now().minusDays(NUMBER_OF_DAYS - 1);
		List<Log> logs = new ArrayList<>();

		String url = "";

		for (int i = 0; i < NUMBER_OF_DAYS; i++) {
			// get date to replace index pattern date. Assumption is YYYY.MM.DD
			String year = String.valueOf(dateOnIteration.getYear());
			String monthFormatted = String.format("%02d", dateOnIteration.getMonthValue());
			String dayFormatted = String.format("%02d", dateOnIteration.getDayOfMonth());

			// this works for [index-]YYYY.MM.DD but what if pattern is
			// different?
			String index = indexPattern.replace("YYYY", year).replace("MM", monthFormatted).replace("DD", dayFormatted);

			url = url + index + "%2C";

			// Subtract a day
			dateOnIteration = dateOnIteration.plusDays(1);
		}

		url = (url.endsWith("%2C")) ? url.substring(0, url.lastIndexOf("%2C")) : url;

		// String url = "/" + index + dates;
		// host +
		// /[index]-2016.11.06,[index]-2016.11.07,[index]-2016.11.08,[index]-2016.11.09,[index]-2016.11.10/_msearch?timeout=0&preference=1478787521161

		HttpPost request = new HttpPost(host + "/" + url + MSEARCH);
		request.addHeader("content-type", "application/json");

		StringEntity params = new StringEntity(searchTemplate, "UTF-8");
		request.setEntity(params);
		try (CloseableHttpClient client = HttpClientBuilder.create()
				.addInterceptorLast(new AWSSigningRequestInterceptor(awsSigner)).build();) {

			HttpResponse response = client.execute(request);

			JSONObject logsResponse = getResponseBody(response);
			JSONArray responses = logsResponse.getJSONArray("responses");
			for (int i = 0; i < responses.length(); i++) {
				JSONObject responseArrayObject = responses.getJSONObject(i);
				JSONObject hits = responseArrayObject.getJSONObject("hits");
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
			}

		} catch (IOException | ParseException e) {
			throw new RSSException("Encountered an error performing search for logs on dates [" + dateOnIteration
					+ " to " + dateOnIteration.plusDays(4) + "] for dashboard [" + dashboard.getTitle() + "]: "
					+ e.getMessage(), e);
		}

		return logs;
	}

	private String setQuery(String searchTemplate, List<String> queries) {
		if (!queries.isEmpty()) {
			String queryList = "";
			for (String query : queries) {
				// sort musts and must nots

				queryList = queryList + query + ", ";
			}

			queryList = queryList.substring(0, queryList.length() - 3);

			return searchTemplate.replace(QUERY, queryList).replace("\\\"", "\"");
		} else {
			searchTemplate = searchTemplate.replace(QUERY, EMPTY_QUERY);
		}
		return searchTemplate;

	}

	private String setQueries(String searchTemplate, Map<String, String> map, Map<String, String> mapNot,
			List<String> queries) {
		String queryList = "";

		// replace queries
		if (!queries.isEmpty()) {
			for (String query : queries) {
				queryList = queryList + query + ", ";
			}
		}

		// replace must filters
		if (!map.isEmpty()) {
			for (Entry<String, String> query : map.entrySet()) {
				queryList = queryList + query.getKey() + ", ";
			}
		} else {
			searchTemplate = searchTemplate.replace(QUERIES + ",", "");
		}

		if (!map.isEmpty() || !queries.isEmpty()) {
			queryList = queryList.substring(0, queryList.length() - 1);
		}

		// replace must-not filters
		String notQueryList = "";
		if (!mapNot.isEmpty()) {
			for (Entry<String, String> query : mapNot.entrySet()) {
				notQueryList = notQueryList + query.getValue() + ", ";
			}
			notQueryList = notQueryList.substring(0, notQueryList.length() - 2);
		} else {
			searchTemplate = searchTemplate.replace(NOTQUERIES + ",", "");
		}

		return searchTemplate.replace(QUERIES, queryList).replace("\\\"", "\"").replace(NOTQUERIES, notQueryList)
				.replace("\\\"", "\"");
	}

	// returns RSS feed for logs
	public SyndFeed createRss(List<Log> logsForRSS, Dashboard dashboard, String host) throws RSSException {

		// create feed entry for each log
		List<SyndEntry> entries = new ArrayList<>();
		for (Log log : logsForRSS) {
			SyndEntry entry;

			SyndContent description;

			entry = new SyndEntryImpl();

			// kibana4:
			// host + "/_plugin/kibana/#/doc/" + [index]-YYYY.MM.DD + actual
			// index + type + "?id=" + id
			String type = log.getType();
			String indexPattern = dashboard.getIndexPattern(); // returns
			// [index-]YYYY.MM.DD
			String id = log.getId();
			String day = String.valueOf(log.getLogDate().getDate());
			String month = String.valueOf(log.getLogDate().getMonth() + 1);// getMonth()
																			// returns
																			// 0-11

			String year = String.valueOf(log.getLogDate().getYear() + 1900);
			String index = indexPattern.replace("[", "").replace("]", "").replace("YYYY", year).replace("MM", month)
					.replace("DD", day);
			// "/_plugin/kibana/#/doc/[@@index@@/@@indexDate@@/@@type@@?id=@@id@@&_g=()"
			entry.setLink(host + LOG_URL.replace(INDEX, indexPattern).replace(INDEX_DATE, index).replace(TYPE, type)
					.replaceAll(ID, id));

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
