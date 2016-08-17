<?php
namespace Flowpack\EventStore\DatabaseStorageAdapter;

/*
 * This file is part of the Flowpack.Cqrs package.
 *
 * (c) Hand crafted with love in each details by medialib.tv
 */

use Flowpack\EventStore\DatabaseStorageAdapter\Factory\ConnectionFactory;
use Flowpack\EventStore\EventStreamData;
use Flowpack\EventStore\Exception\ConcurrencyException;
use Flowpack\EventStore\Storage\EventStorageInterface;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Utility\Algorithms;

/**
 * Database event storage, for testing purpose
 */
class DatabaseEventStorage implements EventStorageInterface
{
    /**
     * @var ConnectionFactory
     * @Flow\Inject
     */
    protected $connectionFactory;

    /**
     * @var array
     */
    protected $runtimeCache = [];

    /**
     * @var array
     */
    protected $runtimeVersionCache = [];

    /**
     * @param string $identifier
     * @return EventStreamData
     */
    public function load(string $identifier)
    {
        $version = $this->getCurrentVersion($identifier);
        $cacheKey = $identifier . '.' . $version;
        if (isset($this->runtimeCache[$cacheKey])) {
            return $this->runtimeCache[$cacheKey];
        }
        $conn = $this->connectionFactory->get();
        $streamName = $this->connectionFactory->getStreamName();
        $queryBuilder = $conn->createQueryBuilder();
        $query = $queryBuilder
            ->select('aggregate_name, version, name, aggregate_identifier, timestamp, payload')
            ->from($streamName)
            ->andWhere('aggregate_identifier = ?')
            ->orderBy('version')
            ->setParameter(0, $identifier);
        $count = $query->execute()->rowCount();
        if ($count === 0) {
            return null;
        }

        $data = [];
        $aggregateName = $version = null;
        foreach ($query->execute()->fetchAll() as $event) {
            $aggregateName = $event['aggregate_name'];
            $version = $event['version'];
            $data[] = [
                'class' => str_replace('.', '\\', $event['name']),
                'aggregate_identifier' => $event['aggregate_identifier'],
                'name' => $event['name'],
                'timestamp' => $event['timestamp'],
                'payload' => json_decode($event['payload'], true),
            ];
        }

        $cacheKey = $identifier . '.' . $version;
        $this->runtimeCache[$cacheKey] = new EventStreamData($identifier, $aggregateName, $data, $version);

        return $this->runtimeCache[$cacheKey];
    }

    /**
     * @param string $identifier
     * @param string $aggregateName
     * @param array $data
     * @param integer $version
     * @throws ConcurrencyException
     */
    public function commit(string $identifier, string $aggregateName, array $data, int $version)
    {
        $stream = new EventStreamData($identifier, $aggregateName, $data, $version);
        $conn = $this->connectionFactory->get();

        $streamName = $this->connectionFactory->getStreamName();

        $queryBuilder = $conn->createQueryBuilder();

        foreach ($stream->getData() as $eventData) {
            $payload = json_encode($eventData['payload'], JSON_PRETTY_PRINT);
            $query = $queryBuilder
                ->insert($streamName)
                ->values([
                    'identifier' => '?',
                    'version' => '?',
                    'name' => '?',
                    'name_hash' => '?',
                    'payload' => '?',
                    'payload_hash' => '?',
                    'timestamp' => '?',
                    'aggregate_identifier' => '?',
                    'aggregate_name' => '?',
                    'aggregate_name_hash' => '?'
                ])
                ->setParameters([
                    Algorithms::generateUUID(),
                    $version,
                    $eventData['name'],
                    md5($eventData['name']),
                    $payload,
                    md5($payload),
                    $eventData['timestamp'],
                    $identifier,
                    $aggregateName,
                    md5($aggregateName)
                ]);
            $query->execute();
        }

        // Update the cache
        $cacheKey = $identifier . '.' . $version;
        $this->runtimeCache[$cacheKey] = $stream;
    }

    /**
     * @param string $identifier
     * @return boolean
     */
    public function contains(string $identifier): bool
    {
        return $this->getCurrentVersion($identifier) > 1 ? true : false;
    }

    /**
     * @param  string $identifier
     * @return integer Current Aggregate Root version
     */
    public function getCurrentVersion(string $identifier): int
    {
        $conn = $this->connectionFactory->get();
        $streamName = $this->connectionFactory->getStreamName();
        $queryBuilder = $conn->createQueryBuilder();
        $query = $queryBuilder
            ->select('version')
            ->from($streamName)
            ->andWhere('aggregate_identifier = ?')
            ->orderBy('version', 'DESC')
            ->setMaxResults(1)
            ->setParameter(0, $identifier);

        $version = (integer)$query->execute()->fetchColumn();
        $this->runtimeVersionCache[$identifier] = $version;
        return $version ?: 1;
    }
}
