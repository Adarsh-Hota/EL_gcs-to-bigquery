An EL data pipeline that leverages the capabilities of Apache Airflow to extract a file from the web and subsequently upload it to BigQuery. 

<table>
  <tr>
    <td><img src="/docs/images/project_overview.png" title="Project Overview" width="600" height="200"/></td>
  </tr>
</table>

To start the process, the DAG (Directed Acyclic Graph) first downloads the desired file from the web source. The downloaded file is then uploaded to Google Cloud Storage. 
In the next step of the process, an external table is created in BigQuery, which refers to the uploaded file in GCS. This table can be used to store and analyze the data.
