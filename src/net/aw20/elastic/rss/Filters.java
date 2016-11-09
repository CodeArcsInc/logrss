package net.aw20.elastic.rss;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Filters {

	private List<String> fields;
	private Map<String, List<String>> values;
	private Map<String, String> filterQueries;

	public Filters() {
		this.fields = new ArrayList<>();
		this.values = new HashMap<>();
		this.filterQueries = new HashMap<>();
	}

	public void add(String type, String field, String value) {
		if ("field".equals(type)) {
			addFilterQuery(field, value);
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

	public List<String> getFields() {
		return this.fields;
	}

	public List<String> getValues(String field) {
		return this.values.get(field);
	}

	public Map<String, String> getFilterQueries() {
		return this.filterQueries;
	}

	public void addFilterQuery(String field, String query) {
		this.filterQueries.put(field, query.replace("\"", "\\\""));
	}
}
