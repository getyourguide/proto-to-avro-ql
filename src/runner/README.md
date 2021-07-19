# About

This Runner provides a quick mechanism to test the library against the GoogleAds API.

To use the runner, you need to provide a HOCON file with credentials at `.env.application.conf`. The format
of the file is:

```
google {
    developer-token = SANITISED,
    client-id = SANITISED,
    client-secret = SANITISED,
    login-customer-id = 0123456789,
    refresh-token = SANITSED
}
```

After executing the runner, the following files are produced:

- `outputs` - the output directory
- `outputs/cached_report.b64` - a cached snapshot of the result of the report, to avoid querying on each run
- `outputs/report.avro` - an avro file produced by applying a translator to the report results
- `outputs/avro-report.json` - a json file produced by going through the library (proto -> avro -> json)
- `outputs/google-report.json` - a json file produced by going directly to json (proto -> json)

This lets you compare the output of the library in avro and json, along with the Google equivalent.

The run will also produce timing information (though this should be taken with a pinch of salt since everything runs in the same JVM).

If you need to redownload the report, simply delete `cached_report.b64`. If you change the query then the report
will also be rerun (since the query is used to check if the data is fresh).