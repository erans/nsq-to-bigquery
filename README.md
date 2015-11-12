# nsq-to-bigquery
Stream NSQ messages to Google's BigQuery using the streaming API.

Written by Eran Sandler ([@erans](https://twitter.com/erans))


This is a very early version that does work but was not (yet) tested in real hard production environment.

## Assumptions / Limitations:
- Message body is a valid JSON that is a flat dictionary (only key and simple values).
- Each message must contain data that will constitute a single row at the end.
- BigQuery table MUST exists and the schema MUST matches the JSON being sent.
- This version sends each message in its own request and does not use any batch API (yet).


