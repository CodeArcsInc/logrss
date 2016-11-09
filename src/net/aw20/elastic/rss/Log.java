package net.aw20.elastic.rss;

import java.util.Date;

public class Log {
	private String index;
	private String type;
	private String id;
	private String message;
	private String toDateParam;
	private String fromDateParam;
	private Date logDate;

	public Log(String id, String index, String type) {
		this.index = index;
		this.type = type;
		this.id = id;
	}

	public String getIndex() {
		return index;
	}

	public String getType() {
		return type;
	}

	public String getId() {
		return id;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getToDateParam() {
		return toDateParam;
	}

	public void setToDateParam(String timestamp) {
		this.toDateParam = timestamp;
	}

	public String getFromDateParam() {
		return fromDateParam;
	}

	public void setFromDateParam(String timestamp) {
		this.fromDateParam = timestamp;
	}

	public Date getLogDate() {
		return logDate;
	}

	public void setLogDate(Date logDate) {
		this.logDate = logDate;
	}

}
