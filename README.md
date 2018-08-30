# db-sync

DB Migration Spark Job using Cloud Dataproc : 
- Supports Full, and Incremental(Append Only) Sync
- Uses JDBC
- Incremental Sync based on Numeric Column
- Uses Externalized HOCON/JSON file for configuration

Supported List of Databases :
- MySQL

To Build the Fat Jar:
```bash
sbt compile && sbt assembly
```

Fat Jar will be available at:

```bash
target/scala-2.11 as db-sync-assembly-<version>.jar
```

To Configure the Migration Job:

* Create a Configuration File
  * Create a JSON out of customized HOCON File.
  * Since we read the file contents all at once, the Configuration file needs to fit in one line.
* Copy the Configuration file to a GCS bucket
  * ```bash
       gsutil cp config.json gs://bucket/config.json
       ```

To Run the Migration Job:
*  Modify the scripts below:
  * create-internal-cluster.sh
  * run-job.sh
  * delete-cluster.sh
* Execute:
  ```bash
    create-internal-cluster.sh && run-job.sh && delete-cluster.sh
    ```
