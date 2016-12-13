package net.aw20.elastic.rss;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Filters {

	private List<String> fields;
	private Map<String, List<String>> values;
	private Map<String, String> filterQueries;
	private Map<String, String> filterQueriesNot;

	public Filters() {
		this.fields = new ArrayList<>();
		this.values = new HashMap<>();
		this.filterQueries = new HashMap<>();
		this.filterQueriesNot = new HashMap<>();
	}

	public void add(String type, String field, String value, boolean negate, int version) {
		if (version == 4) {
			if ("phrase".equals(type)) {
				addFilterQuery(field, value, negate);
			}
		} else {
			if ("field".equals(type)) {
				addFilterQuery(field, value, negate);
			}

			if ("terms".equals(type)) {
				if (!this.fields.contains(field)) {
					this.fields.add(field);
				}

				if (value != null) {
					List<String> list;
					value = "\"" + value + "\"";
					if (this.values.containsKey(field)) {
						this.values.get(field).add(value);
					} else {
						list = new ArrayList<>();
						list.add(value);
						this.values.put(field, list);
					}
				}
			}
		}
	}

	public List<String> getFields() {
		return this.fields;
	}

	public List<String> getValues(String field) {
		return this.values.get(field);
	}

	public Map<String, String> getFilterQueries() {
		return this.filterQueries;
	}

	public Map<String, String> getFilterQueryNots() {
		return this.filterQueriesNot;
	}

	public void addFilterQuery(String field, String query, boolean negate) {
		if (negate) {
			this.filterQueriesNot.put(query.replace("\"", "\\\""), field);
		} else {
			this.filterQueries.put(query.replace("\"", "\\\""), field);
		}
	}
}
