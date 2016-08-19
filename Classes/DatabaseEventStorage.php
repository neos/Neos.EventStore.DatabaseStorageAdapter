<?php
namespace Ttree\EventStore\DatabaseStorageAdapter;

/*
 * This file is part of the Ttree.Cqrs package.
 *
 * (c) Hand crafted with love in each details by medialib.tv
 */

use Doctrine\DBAL\Types\Type;
use Ttree\Cqrs\Domain\Timestamp;
use Ttree\EventStore\DatabaseStorageAdapter\Factory\ConnectionFactory;
use Ttree\EventStore\EventStreamData;
use Ttree\EventStore\Exception\ConcurrencyException;
use Ttree\EventStore\Storage\EventStorageInterface;
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
        $cacheKey = md5($identifier . '.' . $version);
        if (isset($this->runtimeCache[$cacheKey])) {
            return $this->runtimeCache[$cacheKey];
        }
        $conn = $this->connectionFactory->get();
        $streamName = $this->connectionFactory->getStreamName();
        $queryBuilder = $conn->createQueryBuilder();
        $query = $queryBuilder
            ->select('aggregate_name, version, name, aggregate_identifier, timestamp, payload')
            ->from($streamName)
            ->andWhere('aggregate_identifier = :aggregate_identifier')
            ->orderBy('version', 'ASC')
            ->setParameter('aggregate_identifier', $identifier);

        $data = [];
        $aggregateName = null;
        foreach ($query->execute()->fetchAll() as $event) {
            $aggregateName = $event['aggregate_name'];
            $data[] = [
                'class' => str_replace('.', '\\', $event['name']),
                'aggregate_identifier' => $event['aggregate_identifier'],
                'name' => $event['name'],
                'timestamp' => $event['timestamp'],
                'payload' => json_decode($event['payload'], true),
            ];
        }

        if ($data === []) {
            return null;
        }

        $cacheKey = md5($identifier . '.' . $version);
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
            $timestamp = \DateTime::createFromFormat(Timestamp::OUTPUT_FORMAT, $eventData['timestamp']);
            $query = $queryBuilder
                ->insert($streamName)
                ->values([
                    'identifier' => ':identifier',
                    'version' => ':version',
                    'name' => ':name',
                    'name_hash' => ':name_hash',
                    'payload' => ':payload',
                    'payload_hash' => ':payload_hash',
                    'timestamp' => ':timestamp',
                    'microseconds' => ':microseconds',
                    'aggregate_identifier' => ':aggregate_identifier',
                    'aggregate_name' => ':aggregate_name',
                    'aggregate_name_hash' => ':aggregate_name_hash'
                ])
                ->setParameters([
                    'identifier' => Algorithms::generateUUID(),
                    'version' => $version,
                    'name' => $eventData['name'],
                    'name_hash' => md5($eventData['name']),
                    'payload' => $payload,
                    'payload_hash' => md5($payload),
                    'timestamp' => $timestamp,
                    'microseconds' => $timestamp->format('u'),
                    'aggregate_identifier' => $identifier,
                    'aggregate_name' => $aggregateName,
                    'aggregate_name_hash' => md5($aggregateName)
                ], [
                    'identifier' => \PDO::PARAM_STR,
                    'version' => \PDO::PARAM_INT,
                    'name' => \PDO::PARAM_STR,
                    'name_hash' => \PDO::PARAM_STR,
                    'payload' => \PDO::PARAM_STR,
                    'payload_hash' => \PDO::PARAM_STR,
                    'timestamp' => Type::DATETIME,
                    'microseconds' => \PDO::PARAM_INT,
                    'aggregate_identifier' => \PDO::PARAM_STR,
                    'aggregate_name' => \PDO::PARAM_STR,
                    'aggregate_name_hash' => \PDO::PARAM_STR
                ]);
            $query->execute();
        }
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
