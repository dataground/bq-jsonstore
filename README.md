# BigQuery JSON Store
Generic BigQuery immutable, append only storage for arbitrary versioned JSON items by unique key.
Turns BigQuery into a schemaless JSON datastore capable of handling huge datasets.

## Installation
`composer install dataground/bq-jsonstore`

## Write data
```php
$bqbs = new BigQueryBatchService(
    new BigQueryClient([
      'projectId' => 'myproject',
      'keyFile' => json_decode(file_get_contents('my-gcp-key.json'), true)
  ])
 );
  
$bqbs->start('mydataset');
$bqbs->add('mytable', 'something-unique', ['my-fancy-json-thing => ['foo' => 'bar']]);
$bqbs->flush();
```
  
## Read data 
To read latest version of data in BigQuery SQL

```sql
#StandardSQL
SELECT * FROM `mydataset.mytable` AS thetable
INNER JOIN (SELECT uid as cuid, max(id) as maxid FROM `mydataset.mytable` GROUP BY uid) AS latest
ON thetable.uid = latest.cuid AND thetable.id = latest.maxid
```

## Access JSON attributes
Data can be accessed in SQL using JSON_EXTRACT() and JSON_EXTRACT_SCALAR() functions.
Check bigquery documentation for more info!

## TODO
* Autocreate [table]_current (incremental) and [table]_last (latest batch) views on dataset creation
* Add unittests
* More documentation
