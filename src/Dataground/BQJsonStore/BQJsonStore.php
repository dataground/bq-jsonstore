<?php
/**
 * Copyright 2017 Pim Koeman (pim@dataground.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

namespace Dataground\BQJsonStore;

use ErrorException;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Dataset;
use Google\Cloud\BigQuery\Table;
use Google\Cloud\BigQuery\Timestamp;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;
use StephenHill\Base58;

/**
 * Class BQJsonStore
 *
 * @package Dataground\BQJsonStore
 */
class BQJsonStore
{
    const LOCATION_EU = 'EU';
    const LOCATION_US = 'US';
    const EVENT_UPD = 'UPD';
    const EVENT_DEL = 'DEL';
    const PARTITION_DELIMITER = '_';
    const ID_REVISION_DELIMITER = '-';

    /**
     * @var BigQueryClient
     */
    private $bqClient = null;

    /**
     * @var Dataset
     */
    private $bqDataset = null;

    /**
     * @var array
     */
    private $chunks = [];

    /**
     * @var string
     */
    private $projectUri = '';

    /**
     * @var string
     */
    private $datasetUri = '';

    /**
     * @var int
     */
    private $revision = 0;

    /**
     * @var string
     */
    private $locationUri = 'EU';

    /**
     * @var LoggerInterface
     */
    private $logger = null;

    /**
     * @var Base58
     */
    private $base58 = null;

    /**
     * @var string
     */
    private $partitionPostfix = '';

    /**
     * @var string
     */
    private $version = '1.0.0';

    /**
     * BQJsonStore constructor.
     *
     * @param string         $project  Google Cloud Platform Project id
     * @param BigQueryClient $bqClient Google BigQuery Client
     *
     * @throws ErrorException
     */
    public function __construct($project, BigQueryClient $bqClient)
    {
        $this->base58 = new Base58();
        $this->bqClient = $bqClient;
        $this->projectUri = $project;
    }

    /**
     * @param string $locationUri
     */
    public function setLocationUri($locationUri)
    {
        if (!in_array($locationUri, [self::LOCATION_EU, self::LOCATION_US])) {
            throw new InvalidArgumentException(
              'Unknown location (valid :' . self::LOCATION_US . ' or ' . self::LOCATION_EU . ')'
            );
        }

        $this->locationUri = $locationUri;
    }

    /**
     * @param Base58 $base58
     */
    public function setBase58Encoder(Base58 $base58)
    {
        $this->base58 = $base58;
    }

    /**
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    /**
     * @param int $partition
     */
    public function setPartition($partition)
    {
        if (!is_int($partition) || $partition < 1) {
            throw new InvalidArgumentException('Partition key has to be a positive integer numeric value');
        }

        $this->partitionPostfix = self::PARTITION_DELIMITER . $partition;
    }

    /**
     * @param string $version
     */
    public function setVersion($version)
    {
        if (!version_compare($version, '0.0.1', '>=')) {
            throw new InvalidArgumentException('Invalid version number format (expected x.y.z)');
        }

        $this->version = $version;
    }

    /**
     * @param $msg
     */
    private function logInfo($msg)
    {
        if ($this->logger !== null) {
            $this->logger->info($msg);
        }
    }

    /**
     * @return array
     */
    private function getSchema()
    {
        return [
          'fields' => [
            ['name' => 'id', 'type' => 'string', 'mode' => 'required'],
            ['name' => 'revision', 'type' => 'int64', 'mode' => 'required'],
            ['name' => 'parent_uid', 'type' => 'string'],
            ['name' => 'uid', 'type' => 'string', 'mode' => 'required'],
            ['name' => 'hash', 'type' => 'string', 'mode' => 'required'],
            ['name' => 'event', 'type' => 'string', 'mode' => 'required'],
            ['name' => 'version', 'type' => 'string', 'mode' => 'required'],
            ['name' => 'json', 'type' => 'string', 'mode' => 'required']
          ]
        ];
    }

    /**
     * Fetch a row from BQ
     *
     * @param $sql
     *
     * @return mixed
     * @throws ErrorException
     */
    public function fetchRow($sql)
    {

        $queryResults = $this->bqClient->runQuery(
          '#standardSQL' . PHP_EOL .
          $sql
        );

        $retries = 0;

        while (!$queryResults->isComplete()) {

            // Backoff slowly
            usleep(100 * $retries);

            $queryResults->reload();

            $retries++;
            if ($retries > 500) {
                throw new ErrorException('Timeout during query');
            }
        }

        $row = $queryResults->rows()->current();

        return $row;
    }

    /**
     * Get the maximum value of a json member element
     * Used to determine last update date
     *
     * @param $dataset
     * @param $table
     * @param $jsonPath
     *
     * @return bool|string
     */
    public function fetchMaxJsonValue($dataset, $table, $jsonPath)
    {
        if (
          $this->bqClient->dataset($dataset)->exists() &&
          $this->bqClient->dataset($dataset)->table($table)->exists()
        ) {
            $table = $dataset . '.' . $table;
            $sql = "SELECT MAX(JSON_EXTRACT(json, '" . $jsonPath . "')) as maxvalue FROM `" . $table . "` LIMIT 1";
            $row = $this->fetchRow($sql);

            return trim($row['maxvalue'], '"');
        } else {
            return false;
        }
    }

    /**
     * @return int
     * @throws ErrorException
     * @throws \Google\Cloud\Core\Exception\GoogleException
     *
     * Get current timestamp from BigQuery cluster with microsecond precision as INT64
     * Format does not include the first century digit.
     *
     * This leaves a theoretical room until (2)922-12-31 23:59:59 (max INT64 =  9223372036854)
     */
    private function getCurrentTimeStampAsInt64()
    {
        $row = $this->fetchRow('SELECT CURRENT_TIMESTAMP() as now');
        $timestamp = $row['now'];

        if ($timestamp === null || !($timestamp instanceof Timestamp)) {
            throw new ErrorException('Invalid Query Result');
        }

        /** Get datestamp with microseconds, remove first century digit **/
        $result = trim(substr($timestamp->get()->format('YmdHisu'), 1),'0');

        if ($result === '0') {
            throw new ErrorException('Integer value conversion error');
        }

        return $result;
    }


    /**
     * @param $dataset
     */
    public function start($dataset)
    {
        // @todo check dataset uri
        $this->logInfo('Starting batch');

        $this->revision = 0;
        $this->datasetUri = $dataset;
        $this->chunks = [];

        if ($this->bqClient->dataset($dataset)->exists() === false) {

            $this->logInfo('Creating dataset ' . $dataset);
            $this->bqClient->createDataset(
              $dataset,
              ['location' => $this->locationUri]
            );
        }

        $this->bqDataset = $this->bqClient->dataset($dataset);
    }

    /**
     * @param $table
     *
     * @return bool
     */
//    private function createViews($dataset, $table)
//    {
//        $view = $table . '_current';
//
//        $baseTable = $this->projectUri.'.'.$dataset . '.' . $table;
//
//        if (!$this->bqClient->dataset($dataset)->table($view)->exists()) {
//            $sql = "#standardSQL" . PHP_EOL ."
//          select uid, parent_uid, json FROM `" . $baseTable . "` as source
//          INNER JOIN (SELECT uid as cuid, max(id) as maxid FROM `" . $baseTable . "` GROUP BY uid) AS latest
//          ON source.uid = latest.cuid AND source.id = latest.maxid AND source.event != 'DEL'";
//
//            $this->bqClient->dataset($dataset)->createTable($view, ['view' => ['query' => $sql]]);
//        }
//    }

    /**
     * @param       $table
     * @param       $uid
     * @param array $payload
     * @param null  $parentUid
     */
    public function add($table, $uid, array $payload, $parentUid = null)
    {
        if (!isset($this->chunks[$table . $this->partitionPostfix])) {
            $this->chunks[$table . $this->partitionPostfix] = [];
        }

        $json = json_encode($payload, JSON_FORCE_OBJECT);

        $rec = [
          'data' => [
            'uid'        => $uid,
            'parent_uid' => $parentUid,
            'hash'       => '',
            'version'    => $this->version,
            'json'       => $json,
            'event'      => self::EVENT_UPD
          ]
        ];

        $this->chunks[$table . $this->partitionPostfix][] = $rec;
    }

    /**
     * @param $table
     * @param $uid
     */
    public function delete($table, $uid)
    {
        $rec = [
          'data' => [
            'uid'        => $uid,
            'parent_uid' => null,
            'hash'       => '',
            'version'    => $this->version,
            'json'       => '{}',
            'event'      => self::EVENT_DEL
          ]
        ];

        $this->chunks[$table . $this->partitionPostfix][] = $rec;
    }

    /**
     *  Write data to BigQuery tables
     */
    public function flush()
    {
        $this->logInfo('Committing data chunks to BigQuery');

        /**
         * @var Table[] $tables
         */
        $tables = [];

        // Get tables for chunks
        foreach (array_keys($this->chunks) as $table) {
            if ($this->bqDataset->table($table)->exists() === false) {
                $this->logInfo('Creating table ' . $table);
                $tables[$table] = $this->bqDataset->createTable(
                  $table,
                  ['schema' => $this->getSchema()]
                );
            } else {
                $tables[$table] = $this->bqDataset->table($table);
            }
        }

        if ($this->revision === 0) {
            $this->revision = $this->getCurrentTimeStampAsInt64();
        }

        foreach ($this->chunks as $table => $chunk) {

            foreach ($chunk as $index => $item) {
                $chunk[$index]['data']['id'] =
                  $this->revision .
                  self::ID_REVISION_DELIMITER .
                  $chunk[$index]['data']['uid'];

                $chunk[$index]['data']['hash'] = $this->base58->encode(
                  hash('sha256',
                    $chunk[$index]['data']['uid'] .
                    $chunk[$index]['data']['parent_uid'] .
                    $chunk[$index]['data']['version'] .
                    $chunk[$index]['data']['json']
                    , true
                  )
                );

                $chunk[$index]['data']['revision'] = $this->revision;
            }

            $this->logInfo('Writing chunk ' . $table);
            $insertResponse = $tables[$table]->insertRows($chunk);

            if (!$insertResponse->isSuccessful()) {
                $errors = [];

                foreach ($insertResponse->failedRows() as $row) {
                    foreach ($row['errors'] as $error) {
                        $errors[] = $error['reason'] . ': ' . $error['message'] . PHP_EOL;
                    }
                }

                throw new ErrorException('Error while inserting data ' . var_dump($errors, 1));
            }
        }

        $this->chunks = [];

        $this->logInfo('Flush ready');
    }
}