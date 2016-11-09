package net.aw20.elastic.rss;

import java.util.ArrayList;
import java.util.List;

public class Dashboard {

	private String title;
	private String indexPattern;
	private Filters filters;
	private List<String> queries;
	private String link;

	Dashboard(String title, String indexPattern, String link) {
		this.title = title;
		this.indexPattern = indexPattern;
		this.link = link;
		filters = new Filters();
		queries = new ArrayList<>();
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public void addOption(String type, String field, String value) {
		filters.add(type, field, value);
	}

	public List<String> getFields() {
		return this.filters.getFields();
	}

	public List<String> getValuesForField(String field) {
		return this.filters.getValues(field);
	}

	public String getIndexPattern() {
		return indexPattern;
	}

	public String getLink() {
		return link;
	}

	public List<String> getQueries() {
		return queries;
	}

	public void addQuery(String query) {
		this.queries.add(query);
	}

	public Filters getFilters() {
		return this.filters;
	}

}
