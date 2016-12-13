package net.aw20.elastic.rss;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.base.Supplier;
import com.google.common.net.UrlEscapers;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedOutput;

import vc.inreach.aws.request.AWSSigner;

/**
 * Servlet to support getting RSS feeds from Elasticsearch for predefined Kibana
 * dashboards
 */
@WebServlet(urlPatterns = { "/RSS", "/rss" }, loadOnStartup = 1)
public class RssServlet extends HttpServlet {

	private static final Logger LOGGER = LoggerFactory.getLogger(RssServlet.class);

	private static final long serialVersionUID = 1L;

	// Resource Location
	private static final String TEMPLATE_LOC_V3 = "/WEB-INF/searchQueryTemplateV3.txt";
	private static final String TEMPLATE_LOC_V4 = "/WEB-INF/searchQueryTemplateV4.txt";

	// Constants for the AWSSigner
	private static final Supplier<LocalDateTime> CLOCK = () -> LocalDateTime.now(ZoneOffset.UTC);
	private static final String SERVICE = "es";

	// System property keys
	private static final String ES_HOST_SYSTEM_KEY = "es.host";

	private static String dashboardName;
	private static final int DEFAULT_KIBANA_VERSION = 3;

	private DefaultAWSCredentialsProviderChain awsCredentialsProviderChain;

	private String esHostViaSystem, templatePath;

	@Override
	public void init() {

		try {

			
			// Ensure we are able to get the AWS credentials
			 awsCredentialsProviderChain = new DefaultAWSCredentialsProviderChain();
			 AWSCredentials awsCredentails = awsCredentialsProviderChain.getCredentials();
			if (awsCredentails == null || awsCredentails.getAWSAccessKeyId() == null
					|| awsCredentails.getAWSSecretKey() == null) {
				throw new RSSException("Unable to locate AWS credentials, ensure system has been properly configured");
			}

			// Ensure we have been provided the es host
			esHostViaSystem = System.getProperty(ES_HOST_SYSTEM_KEY);
			if (esHostViaSystem == null || esHostViaSystem.trim().isEmpty()) {
				throw new RSSException("Unable to locate elastic search host, ensure correct system property ["
						+ ES_HOST_SYSTEM_KEY + "] is set");
			} else if (!esHostViaSystem.startsWith("http://") && !esHostViaSystem.startsWith("https://")) {
				esHostViaSystem = "https://" + esHostViaSystem;
			}

		} catch (RSSException e) {
			LOGGER.error(e.getMessage(), e);
			throw new RuntimeException(e.getMessage(), e);
		}

	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) {
		try {

			// Figure out the host to use for elasticsearch calls
			String elasticSearchHost = request.getParameter("es_host");
			if (elasticSearchHost == null || elasticSearchHost.trim().isEmpty()) {
				elasticSearchHost = esHostViaSystem;
			} else if (!elasticSearchHost.startsWith("http://") && !elasticSearchHost.startsWith("https://")) {
				elasticSearchHost = "https://" + elasticSearchHost;
			}

			// Get host for link to log
			String logsHost;
			String kibanaHost = request.getParameter("kibana_host");
			if (kibanaHost == null || kibanaHost.trim().isEmpty()) {
				logsHost = request.getServerName();
			} else {
				logsHost = kibanaHost;
			}

			// kibana needs https
			if (!logsHost.startsWith("http://") && !logsHost.startsWith("https://")) {
				logsHost = "https://" + logsHost;
			} else if (logsHost.startsWith("http://")) {
				logsHost = logsHost.replace("http://", "https://");
			}

			// Confirm we got a dashboard to look up
			dashboardName = request.getParameter("dashboard");
			if (dashboardName == null || dashboardName.trim().isEmpty()) {

				LOGGER.warn("Unable to determine dashboard, failing request");
				response.sendError(400, "Must provide parameter [dashboard]");

			} else {

				// get version param
				// if no version available or not 3 or 4, use default
				// DEFAULT_KIBANA_VERSION
				int version;
				String versionParam = request.getParameter("v");
				if (versionParam == null
						|| (Integer.parseInt(versionParam) != 3 && Integer.parseInt(versionParam) != 4)) {
					version = DEFAULT_KIBANA_VERSION;
				} else {
					version = Integer.parseInt(versionParam);
				}
				RSSFeedCreator creator;
				if (version == 3) {
					templatePath = getServletContext().getRealPath(TEMPLATE_LOC_V3);
					if (templatePath == null) {
						throw new RSSException("Unable to locate template resource [" + TEMPLATE_LOC_V3 + "]");
					}
					creator = new RSSFeedCreatorV3();
				} else {
					templatePath = getServletContext().getRealPath(TEMPLATE_LOC_V4);
					if (templatePath == null) {
						throw new RSSException("Unable to locate template resource [" + TEMPLATE_LOC_V4 + "]");
					}

					creator = new RSSFeedCreatorV4();
				}

				// Create the signer to be used for subsequent requests to ES
				// server
				AWSSigner awsSigner = new AWSSigner(awsCredentialsProviderChain, getRegionFromHost(elasticSearchHost),
						SERVICE, CLOCK);

				// Get the dashboard information
				JSONObject dashboardResponse = creator.getDashboard(elasticSearchHost,
						encodeUrl(dashboardName, version), dashboardName, awsSigner);
				Dashboard dashboard = creator.parseDashboard(dashboardResponse, logsHost, elasticSearchHost, awsSigner);

				if (dashboard != null) {
					// Get the logs associated to the dashboard from ES
					List<Log> logsForRSS = creator.doSearch(dashboard, elasticSearchHost, templatePath, awsSigner);

					// Convert logs to RSS feed format and output them
					SyndFeed feed = creator.createRss(logsForRSS, dashboard, logsHost);
					response.setContentType("text/xml;charset=UTF-8");

					SyndFeedOutput output = new SyndFeedOutput();
					output.output(feed, response.getWriter());
				} else {
					LOGGER.warn("Dashboard: " + dashboardName + " not found. Returned 404.");
					response.getWriter().append("Dashboard: " + dashboardName + " not found");
					response.setStatus(404);
				}

			}

		} catch (FeedException e) {
			LOGGER.error("Encountered an error attempting to respond with RSS feed output : " + e.getMessage(), e);
			response.setStatus(500);
		} catch (RSSException | IOException e) {
			LOGGER.error(e.getMessage(), e);
			response.setStatus(500);
		}
	}

	// in kibana 4, spaces in dashboard names are replaced with "-"
	private String encodeUrl(String url, int version) {
		if (version == 3) {
			url = UrlEscapers.urlFragmentEscaper().escape(url);
		} else {
			url = url.replace(" ", "-");
		}
		return url;
	}

	private String getRegionFromHost(String host) throws RSSException {

		int esIndex = host.indexOf(".es.amazonaws.com");
		if (esIndex < 0) {
			throw new RSSException("Region cannot be parsed from endpoint address [" + host
					+ "], must end in .<region>.es.amazonaws.com");
		}

		int regionIndex = host.lastIndexOf(".", esIndex - 1);
		if (regionIndex < 0) {
			throw new RSSException("Region cannot be parsed from endpoint address [" + host
					+ "], must end in .<region>.es.amazonaws.com");
		}

		return host.substring(regionIndex + 1, esIndex);

	}
}
