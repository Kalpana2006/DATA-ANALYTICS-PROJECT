# Power BI - Streaming Dashboard Instructions

Option A — Power BI streaming dataset (push):
1. In Power BI Service: Create -> Streaming dataset -> API.
2. Define fields: location_id (Text), lat (Number), lon (Number), vehicle_count_sum (Number), avg_speed_mean (Number), congestion_index (Number), window_end (DateTime).
3. Use the API credentials (dataset push URL) to POST aggregated records from your API or Spark job.
4. In Power BI Desktop, use 'Get Data -> Web' to pull from your API for historical data, or use the streaming dataset for live tiles in a dashboard.
5. Suggested visuals:
   - Map (latitude, longitude) with congestion_index as size.
   - Gauge for overall congestion across all locations.
   - Line chart for avg_speed_mean over time.
   - Table for top N congested locations.

Option B — DirectQuery:
- Export aggregates into a SQL database and connect Power BI via DirectQuery for near-real-time updates.

Push example (Python):
```python
import requests, json
url = '<your_powerbi_push_url>'
payload = [{
    'location_id':'NH48-01','lat':23.02,'lon':72.57,'vehicle_count_sum':240,'avg_speed_mean':28.3,'congestion_index':8.48,'window_end':'2025-12-06T14:32:00Z'
}]
r = requests.post(url, data=json.dumps(payload), headers={'Content-Type':'application/json'})
print(r.status_code, r.text)
```
