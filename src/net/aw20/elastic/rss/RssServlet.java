package net.aw20.elastic.rss;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.google.common.base.Supplier;
import com.google.common.net.UrlEscapers;
import com.sun.syndication.feed.synd.SyndContent;
import com.sun.syndication.feed.synd.SyndContentImpl;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndEntryImpl;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.feed.synd.SyndFeedImpl;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedOutput;

import vc.inreach.aws.request.AWSSigner;
import vc.inreach.aws.request.AWSSigningRequestInterceptor;


/**
 * Servlet to support getting RSS feeds from Elasticsearch for predefined Kibana
 * dashboards
 */
@WebServlet( urlPatterns = { "/RSS", "/rss" }, loadOnStartup = 1 )
public class RssServlet extends HttpServlet {

	private static final Logger LOGGER = LoggerFactory.getLogger( RssServlet.class );

	private static final long serialVersionUID = 1L;

	// ES Templates
	private static final String TERMS_TEMPLATE = "{\"terms\":{}}";
	private static final String QUERY_TEMPLATE = "{\"query_string\": {}}";
	private static final String FILTER_QUERY_TEMPLATE = "{\"fquery\": {\"query\": {\"query_string\": {}},\"_cache\": true}}";

	// ES Keys
	private static final String TERMS = "@@terms@@";
	private static final String QUERIES = "@@queries@@";
	private static final String TO = "@@to@@";
	private static final String FROM = "@@from@@";

	// Resource Location
	private static final String TEMPLATE_LOC = "/WEB-INF/searchQueryTemplate.txt";

	// Kibana URLs
	private static final String KIBANA_INTERNAL_URL = "/kibana-int/dashboard/";
	private static final String KIBANA_DASHBOARD_URL = "/_plugin/kibana3/#/dashboard/elasticsearch/";
	private static final String LOG_URL = "/_plugin/kibana3/#/dashboard/elasticsearch/RSS";

	// Constants for the AWSSigner
	private static final Supplier<LocalDateTime> CLOCK = () -> LocalDateTime.now( ZoneOffset.UTC );
	private static final String SERVICE = "es";

	// System property keys
	private static final String ES_HOST_SYSTEM_KEY = "es.host";

	// Miscellaneous
	private static final int MAX_LOGS = 50;
	private static final int NUMBER_OF_DAYS = 4;
	private static final long FROM_TIME = TimeUnit.DAYS.toMillis( 4 );
	private static String dashboardName;

	private AWSCredentialsProviderChain awsCredentialsProviderChain;

	private String esHostViaSystem, templatePath;


	@Override
	public void init() {

		try {
			AWSCredentials awsCredentails = new BasicAWSCredentials("AKIAIGOFTD2CINPXGCAQ", "Elg9wslme/5keHgZIzhRQlWrNxZPIzMgpGkZP9NH");
			AWSCredentialsProvider acp = new StaticCredentialsProvider(awsCredentails);
			// Ensure we are able to get the AWS credentials
			//awsCredentialsProviderChain = new DefaultAWSCredentialsProviderChain();
			awsCredentialsProviderChain = new AWSCredentialsProviderChain(acp);
			//AWSCredentials awsCredentails = awsCredentialsProviderChain.getCredentials();
			if ( awsCredentails == null || awsCredentails.getAWSAccessKeyId() == null || awsCredentails.getAWSSecretKey() == null ) {
				throw new RSSException( "Unable to locate AWS credentials, ensure system has been properly configured" );
			}

			// Ensure we have been provided the es host
			esHostViaSystem = System.getProperty( ES_HOST_SYSTEM_KEY );
			esHostViaSystem = "search-dev-es-atp-t56t4f3cspzysmdoud3c67x3bm.us-west-2.es.amazonaws.com";
			
			if ( esHostViaSystem == null || esHostViaSystem.trim().isEmpty() ) {
				throw new RSSException( "Unable to locate elastic search host, ensure correct system property [" + ES_HOST_SYSTEM_KEY + "] is set" );
			} else if ( !esHostViaSystem.startsWith( "http://" ) && !esHostViaSystem.startsWith( "https://" ) ) {
				esHostViaSystem = "https://" + esHostViaSystem;
			}

			templatePath = getServletContext().getRealPath( TEMPLATE_LOC );
			if ( templatePath == null ) {
				throw new RSSException( "Unable to locate template resource [" + TEMPLATE_LOC + "]" );
			}

		} catch ( RSSException e ) {
			LOGGER.error( e.getMessage(), e );
			throw new RuntimeException( e.getMessage(), e );
		}

	}


	@Override
	protected void doGet( HttpServletRequest request, HttpServletResponse response ) {
		try {

			// Figure out the host to use for elasticsearch calls
			String elasticSearchHost = request.getParameter( "es_host" );
			if ( elasticSearchHost == null || elasticSearchHost.trim().isEmpty() ) {
				elasticSearchHost = esHostViaSystem;
			} else if ( !elasticSearchHost.startsWith( "http://" ) && !elasticSearchHost.startsWith( "https://" ) ) {
				elasticSearchHost = "https://" + elasticSearchHost;
			}

			// Get host for link to log
			String logsHost;
			String kibanaHost = request.getParameter( "kibana_host" );
			if ( kibanaHost == null || kibanaHost.trim().isEmpty() ) {
				logsHost = request.getServerName();
			} else {
				logsHost = kibanaHost;
			}

			// kibana needs https
			if ( !logsHost.startsWith( "http://" ) && !logsHost.startsWith( "https://" ) ) {
				logsHost = "https://" + logsHost;
			} else if ( logsHost.startsWith( "http://" ) ) {
				logsHost = logsHost.replace( "http://", "https://" );
			}

			// Confirm we got a dashboard to look up
			dashboardName = request.getParameter( "dashboard" );
			if ( dashboardName == null || dashboardName.trim().isEmpty() ) {

				LOGGER.warn( "Unable to determine dashboard, failing request" );
				response.sendError( 400, "Must provide parameter [dashboard]" );

			} else {

				// Create the signer to be used for subsequent requests to ES
				// server
				AWSSigner awsSigner = new AWSSigner( awsCredentialsProviderChain, getRegionFromHost( elasticSearchHost ), SERVICE, CLOCK );

				// Get the dashboard information
				JSONObject dashboardResponse = getDashboardFromElasticsearch( elasticSearchHost, dashboardName, awsSigner );
				Dashboard dashboard = parseDashboard( dashboardResponse, logsHost );

				// Get the logs associated to the dashboard from ES
				List<Log> logsForRSS = doSearch( dashboard, elasticSearchHost, templatePath, awsSigner );

				// Convert logs to RSS feed format and output them
				SyndFeed feed = createRss( logsForRSS, dashboard, logsHost );
				response.setContentType( "text/xml;charset=UTF-8" );

				SyndFeedOutput output = new SyndFeedOutput();
				output.output( feed, response.getWriter() );

			}

		} catch ( FeedException e ) {
			LOGGER.error( "Encountered an error attempting to respond with RSS feed output : " + e.getMessage(), e );
			response.setStatus( 500 );
		} catch ( RSSException | IOException e ) {
			LOGGER.error( e.getMessage(), e );
			response.setStatus( 500 );
		}
	}


	private String getRegionFromHost( String host ) throws RSSException {

		int esIndex = host.indexOf( ".es.amazonaws.com" );
		if ( esIndex < 0 ) {
			throw new RSSException( "Region cannot be parsed from endpoint address [" + host + "], must end in .<region>.es.amazonaws.com" );
		}

		int regionIndex = host.lastIndexOf( ".", esIndex - 1 );
		if ( regionIndex < 0 ) {
			throw new RSSException( "Region cannot be parsed from endpoint address [" + host + "], must end in .<region>.es.amazonaws.com" );
		}

		return host.substring( regionIndex + 1, esIndex );

	}


	private JSONObject getDashboardFromElasticsearch( String hostname, String dashboard, AWSSigner awsSigner ) throws RSSException {

		String queryUrl = getQueryUrl( dashboard, hostname );
		HttpGet request = new HttpGet( queryUrl );
		try ( CloseableHttpClient client = HttpClientBuilder.create().addInterceptorLast( new AWSSigningRequestInterceptor( awsSigner ) ).build(); ) {

			HttpResponse response = client.execute( request );
			if ( response.getStatusLine().getStatusCode() >= 300 ) {
				throw new RSSException( "Received response code [" + response.getStatusLine().getStatusCode() + "] when attempting to look up dashboard [" + dashboard + "] on elasticsearch - response [" + getResponseBody( response ) + "]" );
			}
			return getResponseBody( response );

		} catch ( IOException e ) {
			throw new RSSException( "Encountered an error attempting to get dashboard from elasticsearch : " + e.getMessage(), e );
		}

	}


	private String getQueryUrl( String dashboard, String hostname ) {
		// NOTE : do not use URLEncoder.encode() because it uses + instead of
		// %20 for spaces which throws off the authorization
		String dashboardName = UrlEscapers.urlFragmentEscaper().escape( dashboard );
		return hostname + KIBANA_INTERNAL_URL + dashboardName;
	}


	private JSONObject getResponseBody( HttpResponse response ) throws IOException {
		// Get the response
		StringBuilder builder = new StringBuilder();

		try ( InputStreamReader isr = new InputStreamReader( response.getEntity().getContent() ); BufferedReader rd = new BufferedReader( isr ); ) {

			String aux = "";
			while ( ( aux = rd.readLine() ) != null ) {
				builder.append( aux );
			}

		}

		System.out.println( response );

		return new JSONObject( builder.toString() );
	}


	private Dashboard parseDashboard( JSONObject json, String host ) throws RSSException {

		try {
			/*
			 * { "_source": { "user": "guest", "group": "guest", "title":
			 * "Build Logs", "dashboard": { "title": "BuildLogs", "services":
			 * {}}}}
			 */
			JSONObject sourceObject = json.getJSONObject( "_source" );
			String dashboardString = sourceObject.getString( "dashboard" );
			JSONObject dashboardObject = new JSONObject( dashboardString );
			JSONObject servicesObject = dashboardObject.getJSONObject( "services" );

			// Get index pattern for log file naming ie "[build-]YYYY.MM.DD"
			// This is used for POST request like host/index/_search?timeout=3m{
			JSONObject indexObject = dashboardObject.getJSONObject( "index" );
			String pattern = (String) indexObject.get( "pattern" );

			// Create Dashboard object to contain dashboard data including
			// filter options
			String title = dashboardObject.getString( "title" );
			Dashboard dashboard = new Dashboard( title, pattern, host + KIBANA_DASHBOARD_URL + title );

			// Get list of filters
			JSONObject filterObject = servicesObject.getJSONObject( "filter" );
			JSONArray filterCount = filterObject.getJSONArray( "ids" );
			JSONObject filterListObject = filterObject.getJSONObject( "list" );

			for ( int i = 0; i < filterCount.length(); i++ ) {
				int id = filterCount.getInt( i );
				JSONObject filter = filterListObject.getJSONObject( Integer.toString( id ) );
				if ( !"time".equals( filter.getString( "type" ) ) && filter.getBoolean( "active" ) ) {
					// add filters to dashboard object
					if ( filter.has( "value" ) ) {
						dashboard.addOption( filter.getString( "type" ), filter.getString( "field" ), filter.getString( "value" ) );
					}
					if ( filter.has( "query" ) ) {
						dashboard.addOption( filter.getString( "type" ), filter.getString( "field" ), filter.getString( "query" ) );
					}

				}
			}

			// Get list of lucene queries
			JSONObject queryObject = servicesObject.getJSONObject( "query" );
			JSONArray queryCount = queryObject.getJSONArray( "ids" );
			JSONObject queryListObject = queryObject.getJSONObject( "list" );

			for ( int i = 0; i < queryCount.length(); i++ ) {
				int id = queryCount.getInt( i );
				JSONObject query = queryListObject.getJSONObject( Integer.toString( id ) );
				if ( !"time".equals( query.getString( "type" ) ) && query.getBoolean( "enable" ) ) {
					// add filters to dashboard object
					dashboard.addQuery( query.getString( "query" ) );
				}
			}

			return dashboard;
		} catch ( JSONException e ) {
			throw new RSSException( "Encountered an error parsing dashboard [" + json + "] : " + e.getMessage(), e );
		}

	}


	// return response from post
	private List<Log> doSearch( Dashboard dashboard, String host, String templatePath, AWSSigner awsSigner ) throws RSSException {
		JSONObject dashboardQuery = createSearchQuery( dashboard, templatePath );

		// take dashboard index and replace date. ASSUME date format is
		// YYYY.MM.DD
		// start with most recent date query logs
		// if less than 50 logs for first date, query previous day until we have
		// a list of 50
		// url host + index + /_search?timeout=3m

		LocalDate dateOnIteration = LocalDate.now();
		List<Log> logs = new ArrayList<>();

		// first request todays logs then minus a day on each iteration
		for ( int i = 0; i < NUMBER_OF_DAYS; i++ ) {

			// get date to replace index pattern date. Assumption is YYYY.MM.DD
			String year = String.valueOf( dateOnIteration.getYear() );
			String monthFormatted = String.format( "%02d", dateOnIteration.getMonthValue() );
			String dayFormatted = String.format( "%02d", dateOnIteration.getDayOfMonth() );

			// this works for [index-]YYYY.MM.DD but what if pattern is
			// different?
			String index = dashboard.getIndexPattern().replace( "YYYY", year ).replace( "MM", monthFormatted ).replace( "DD", dayFormatted ).replace( "[", "" ).replace( "]", "" );

			// create url for searching logs
			
			String logSearchUrl = host + "/" + index + "/_search";

			HttpPost request = new HttpPost( logSearchUrl );
			request.addHeader( "content-type", "application/json" );

			StringEntity params = new StringEntity( dashboardQuery.toString(), "UTF-8" );
			request.setEntity( params );

			try ( CloseableHttpClient client = HttpClientBuilder.create().addInterceptorLast( new AWSSigningRequestInterceptor( awsSigner ) ).build(); ) {

				HttpResponse response = client.execute( request );

				JSONObject logsResponse = getResponseBody( response );
				JSONObject hits = logsResponse.getJSONObject( "hits" );
				JSONArray hitsArray = hits.getJSONArray( "hits" );
				if ( hits.getInt( "total" ) != 0 ) {
					for ( int j = 0; j < hitsArray.length(); j++ ) {
						// if we have enough logs, break out
						if ( logs.size() == MAX_LOGS ) {
							break;
						}

						// add log to list
						logs.add( createLog( (JSONObject) hitsArray.get( j ) ) );
					}
				}

			} catch ( IOException | ParseException e ) {
				throw new RSSException( "Encountered an error performing search for logs on date [" + year + "." + monthFormatted + "." + dayFormatted + "] for dashboard [" + dashboard.getTitle() + "]: " + e.getMessage(), e );
			}

			// if we have enough logs, break out
			if ( logs.size() == MAX_LOGS ) {
				break;
			}

			// Subtract a day
			dateOnIteration = dateOnIteration.minusDays( 1 );

		}

		return logs;
	}


	private Log createLog( JSONObject hit ) throws ParseException {

		// get log data
		Log log = new Log( hit.getString( "_id" ), hit.getString( "_index" ), hit.getString( "_type" ) );

		// get log source data
		JSONObject source = hit.getJSONObject( "_source" );

		if ( source.has( "message" ) ) {
			// trim message to 200
			String s = source.getString( "message" );
			log.setMessage( s.length() > 200 ? s.substring( 0, 199 ) : s );
		}

		if ( source.has( "@timestamp" ) ) {
			String s = source.getString( "@timestamp" ).replace( "T", " " ).replace( "Z", "" );
			SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );
			sdf.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
			Date date = sdf.parse( s );

			// subtract 1 day
			String dateBefore = sdf.format( new Date( date.getTime() - 1 * 24 * 3600 * 1000l ) ).replace( " ", "T" );
			log.setFromDateParam( dateBefore );

			// add 1 day
			String dateAfter = sdf.format( new Date( date.getTime() + 1 * 24 * 3600 * 1000l ) ).replace( " ", "T" );
			log.setToDateParam( dateAfter );

			log.setLogDate( date );
		}

		return log;
	}


	private String getSearchQueryTemplate( String templatePath ) throws RSSException {
		char c;
		int i;

		try ( FileInputStream fis = new FileInputStream( templatePath ); InputStreamReader isr = new InputStreamReader( fis ); ) {
			StringBuilder sb = new StringBuilder();

			// read till the end of the file
			while ( ( i = isr.read() ) != -1 ) {
				// int to character
				c = (char) i;
				sb.append( c );
			}
			return sb.toString();
		} catch ( IOException e ) {
			throw new RSSException( "Encountered an error attempting to read search query template : " + e.getMessage(), e );
		}
	}


	private String setFilters( String searchTemplate, Dashboard dashboard ) {
		// add filters
		String filterList = "";
		for ( String field : dashboard.getFields() ) {
			JSONObject terms = new JSONObject( TERMS_TEMPLATE );
			JSONObject termsObject = terms.getJSONObject( "terms" );
			// need to accomplish "host.raw": ["build03", "build02"]
			String[] valueArray = new String[dashboard.getValuesForField( field ).size()];
			int count = 0;
			for ( String value : dashboard.getValuesForField( field ) ) {
				// create array string of all values for field
				valueArray[count] = value;
				count++;
			}
			// add to terms here
			termsObject.put( field, new JSONArray( Arrays.toString( valueArray ) ) );
			filterList = filterList + terms.toString() + ", ";

		}

		// add filter queries to filter FILTER_QUERY_TEMPLATE
		for ( Map.Entry<String, String> query : dashboard.getFilters().getFilterQueries().entrySet() ) {
			JSONObject queries = new JSONObject( FILTER_QUERY_TEMPLATE );
			JSONObject fqueryObject = queries.getJSONObject( "fquery" );
			JSONObject queryObject = fqueryObject.getJSONObject( "query" );
			JSONObject queryStringObject = queryObject.getJSONObject( "query_string" );
			/*
			 * "fquery": { "query": { "query_string": { "query":
			 * "id=aeaergqe4g3" } }, "_cache": true }
			 */
			queryStringObject.put( "query", query.getKey() + ":(" + query.getValue() + ")" );
			filterList = filterList + queries + ", ";
		}

		// remove trailing comma
		filterList = filterList.substring( 0, filterList.length() - 2 );

		// replace @@terms@@
		return searchTemplate.replace( TERMS, filterList ).replace( "\\\"", "\"" );
	}


	private String setQueries( String searchTemplate, Dashboard dashboard ) {
		JSONObject queryTemplate;
		JSONObject queryObject;
		String queryList = "";
		for ( String query : dashboard.getQueries() ) {
			queryTemplate = new JSONObject( QUERY_TEMPLATE );
			queryObject = queryTemplate.getJSONObject( "query_string" );
			/*
			 * "should": [{ "query_string": { "query": "host.raw = build03" } },
			 * { "query_string": { "query": "host.raw = build02" } }]
			 */
			// add to queries here
			queryObject.put( "query", query );
			queryList = queryList + queryTemplate.toString() + ", ";

		}
		queryList = queryList.substring( 0, queryList.length() - 2 );
		// replace @@queries@@
		return searchTemplate.replace( QUERIES, queryList ).replace( "\\\"", "\"" );
	}


	// return json object
	private JSONObject createSearchQuery( Dashboard dashboard, String templatePath ) throws RSSException {

		// load template
		String searchTemplate = getSearchQueryTemplate( templatePath );

		// add filters to template search json
		if ( !dashboard.getFields().isEmpty() ) {
			searchTemplate = setFilters( searchTemplate, dashboard );
		} else {
			// remove ,@@terms@@ because it throws an exception if an empty
			// terms object is there
			searchTemplate = searchTemplate.replace( "," + TERMS, "" );
		}

		// add queries to template search json
		if ( !dashboard.getQueries().isEmpty() ) {
			searchTemplate = setQueries( searchTemplate, dashboard );
		} else {
			// remove ,@@queries@@ because it throws an exception if an empty
			// terms object is there
			searchTemplate = searchTemplate.replace( "," + QUERIES, "" );
		}

		// replace @@to@@ and @@from@@
		searchTemplate = searchTemplate.replace( TO, String.valueOf( System.currentTimeMillis() ) );
		searchTemplate = searchTemplate.replace( FROM, String.valueOf( System.currentTimeMillis() - FROM_TIME ) );

		return new JSONObject( searchTemplate );

	}


	// returns RSS feed for logs
	private SyndFeed createRss( List<Log> logsForRSS, Dashboard dashboard, String host ) throws RSSException {

		// create feed entry for each log
		List<SyndEntry> entries = new ArrayList<>();
		for ( Log log : logsForRSS ) {
			SyndEntry entry;

			SyndContent description;

			entry = new SyndEntryImpl();
			entry.setTitle( log.getMessage() );

			// host + LOG_URL + dashboardName +
			// ?id=AVcPc6cjo9Gp7GaCqRKA&from=2016-09-12T09:15:12Z&to=2016-09-12T09:15:12Z
			// subtract day from from date and add day to to date
			entry.setLink( host + LOG_URL + "?id=" + log.getId() + "&from=" + log.getFromDateParam() + "&to=" + log.getToDateParam() );
			entry.setPublishedDate( log.getLogDate() );
			description = new SyndContentImpl();
			description.setType( "text/plain" );
			description.setValue( log.getMessage() );
			entry.setDescription( description );
			entries.add( entry );
		}

		// create feed
		SyndFeed feed = new SyndFeedImpl();

		feed.setFeedType( "rss_2.0" );
		feed.setTitle( host + " - " + dashboard.getTitle() );
		feed.setLink( dashboard.getLink() );
		feed.setDescription( "This feed has been created for logs from dashboard: " + dashboard.getTitle() );
		feed.setEntries( entries );
		return feed;

	}

}
