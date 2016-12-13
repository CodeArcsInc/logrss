# logrss
A simple Java servlet that provides RSS support for logs using the ELK stack

## Support for Kibana 3:

Setup:
- Create dashboard called RSS and execute a search on the \_id field by clicking on the magnifying glass icon on a log
- Save this dashboard
- In the Save menu, go to Advanced and select Export Schema
- Add .json to the end of this filename and open to edit
- Replace the “filter” section of the JSON with:
```
"filter": {
	"list": {
		 "0": {
			"type": "time",
			"field": "@timestamp",
			"from": "{{ARGS.from}}",
			"to": "{{ARGS.to}}",
			"mandate": "must",
			"active": true,
			"alias": "",
			"id": 0
		},
		"1": {
			"type": "field",
			"field": "_id",
			"query": "{{ARGS.id}}",
			"mandate": "must",
			"active": true,
			"alias": "",
			"id": 1
		}
	},
	"ids": [
		0,
		1
	]
}
```
- Save this dashboard json and reupload to Kibana using the Advanced section in the Load menu. If this section is unavailable, go to the dashboard settings -> Controls and select the “Local file” Load from option
- Save this dashboard again
- Create the dashboard with logs you want to see in an RSS feed.

Usage:

{url}/RSS?dashboard={dashboardName}
	
{url}/RSS?dashboard={DashboardName}&v=3


## Support for Kibana 4:

Setup:
- Create any dashboard with a search in Kibana 4

Usage:

{url}/RSS?dashboard={DashboardName}&v=4
