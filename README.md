# BigQuery JSON Store
Generic BigQuery immutable, append only storage for arbitrary versioned JSON items by unique key.
Turns BigQuery into a schemaless immutable JSON datastore capable of handling huge datasets.

## Installation
`composer install dataground/bq-jsonstore`

## Write data
```php
$bqbs = new BigQueryBatchService(
    'myproject',
    new BigQueryClient([
      'projectId' => 'myproject',
      'keyFile' => json_decode(file_get_contents('my-gcp-key.json'), true)
  ])
);
  
$bqbs->start('mydataset');
$bqbs->add('mytable', 'something-unique', ['my-fancy-json-thing => ['foo' => 'bar']]);
$bqbs->delete('mytable', 'other-unique-thing');
$bqbs->flush();
```
  
## Read data / Access JSON attributes
Data can be accessed in SQL using JSON_EXTRACT() and JSON_EXTRACT_SCALAR() functions. 
Check bigquery documentation for more info.

## Known Issues
This approach does not support updating or deleting items (same uid's) multiple times in one revision. 
Doing this wil lead to unexpected results. To update or delete data, use a separate revision.

## TODO
* Autocreate [table]_last view on dataset creation
    * To read latest combined data use the autocreated view `[mytable]_current` (for incremental loads).
    * To read last imported revision of data use the autocreated view `[mytable]_last` (for full loads).
    
* Prevent multiple uid updates per revision
* Check revision allready exists
* Add unittests
* More documentation
