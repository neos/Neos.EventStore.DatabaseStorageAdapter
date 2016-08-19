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
        $commitName = $this->connectionFactory->getCommitName();
        $queryBuilder = $conn->createQueryBuilder();
        $query = $queryBuilder
            ->select('version, data, created_at, created_at_microseconds, aggregate_identifier, aggregate_name')
            ->from($commitName)
            ->andWhere('aggregate_identifier = :aggregate_identifier')
            ->orderBy('version', 'ASC')
            ->setParameter('aggregate_identifier', $identifier);

        $aggregateName = null;
        $data = [];
        foreach ($query->execute()->fetchAll() as $commit) {
            if ($aggregateName === null) {
                $aggregateName = $commit['aggregate_name'];
            }
            $commitData = json_decode($commit['data'], true);
            $data = array_merge($data, $commitData);
        }
        if ($aggregateName === null) {
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

        $commitName = $this->connectionFactory->getCommitName();

        $queryBuilder = $conn->createQueryBuilder();

        $streamData = [];
        foreach ($stream->getData() as $eventData) {
            $streamData[] = $eventData;
        }

        $now = Timestamp::create();

        $streamData = json_encode($streamData, JSON_PRETTY_PRINT);
        $query = $queryBuilder
            ->insert($commitName)
            ->values([
                'version' => ':version',
                'data' => ':data',
                'data_hash' => ':data_hash',
                'created_at' => ':created_at',
                'created_at_microseconds' => ':created_at_microseconds',
                'aggregate_identifier' => ':aggregate_identifier',
                'aggregate_name' => ':aggregate_name',
                'aggregate_name_hash' => ':aggregate_name_hash'
            ])
            ->setParameters([
                'version' => $version,
                'data' => $streamData,
                'data_hash' => md5($streamData),
                'created_at' => $now,
                'created_at_microseconds' => $now->format('u'),
                'aggregate_identifier' => $identifier,
                'aggregate_name' => $aggregateName,
                'aggregate_name_hash' => md5($aggregateName)
            ], [
                'version' => \PDO::PARAM_INT,
                'data' => \PDO::PARAM_STR,
                'data_hash' => \PDO::PARAM_STR,
                'created_at' => Type::DATETIME,
                'created_at_microseconds' => \PDO::PARAM_INT,
                'aggregate_identifier' => \PDO::PARAM_STR,
                'aggregate_name' => \PDO::PARAM_STR,
                'aggregate_name_hash' => \PDO::PARAM_STR
            ]);

        $query->execute();

        $this->commitStream($version, $identifier, $aggregateName, $stream);
    }

    /**
     * @param string $commitVersion
     * @param string $aggregateIdentifier
     * @param string $aggregateName
     * @param EventStreamData $streamData
     */
    protected function commitStream(string $commitVersion, string $aggregateIdentifier, string $aggregateName, EventStreamData $streamData)
    {
        $conn = $this->connectionFactory->get();
        $queryBuilder = $conn->createQueryBuilder();
        $streamName = $this->connectionFactory->getStreamName();
        $version = 1;
        foreach ($streamData->getData() as $eventData) {
            $payload = json_encode($eventData['payload'], JSON_PRETTY_PRINT);
            $timestamp = \DateTime::createFromFormat(Timestamp::OUTPUT_FORMAT, $eventData['created_at']);
            $query = $queryBuilder
                ->insert($streamName)
                ->values([
                    'identifier' => ':identifier',
                    'commit_version' => ':commit_version',
                    'version' => ':version',
                    'name' => ':name',
                    'name_hash' => ':name_hash',
                    'payload' => ':payload',
                    'payload_hash' => ':payload_hash',
                    'created_at' => ':created_at',
                    'created_at_microseconds' => ':created_at_microseconds',
                    'aggregate_identifier' => ':aggregate_identifier',
                    'aggregate_name' => ':aggregate_name',
                    'aggregate_name_hash' => ':aggregate_name_hash'
                ])
                ->setParameters([
                    'identifier' => Algorithms::generateUUID(),
                    'commit_version' => $commitVersion,
                    'version' => $version,
                    'name' => $eventData['name'],
                    'name_hash' => md5($eventData['name']),
                    'payload' => $payload,
                    'payload_hash' => md5($payload),
                    'created_at' => $timestamp,
                    'created_at_microseconds' => $timestamp->format('u'),
                    'aggregate_identifier' => $aggregateIdentifier,
                    'aggregate_name' => $aggregateName,
                    'aggregate_name_hash' => md5($aggregateName)
                ], [
                    'identifier' => \PDO::PARAM_STR,
                    'commit_version' => \PDO::PARAM_STR,
                    'version' => \PDO::PARAM_INT,
                    'name' => \PDO::PARAM_STR,
                    'name_hash' => \PDO::PARAM_STR,
                    'payload' => \PDO::PARAM_STR,
                    'payload_hash' => \PDO::PARAM_STR,
                    'created_at' => Type::DATETIME,
                    'created_at_microseconds' => \PDO::PARAM_INT,
                    'aggregate_identifier' => \PDO::PARAM_STR,
                    'aggregate_name' => \PDO::PARAM_STR,
                    'aggregate_name_hash' => \PDO::PARAM_STR
                ]);
            $query->execute();
            $version++;
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
        $commitName = $this->connectionFactory->getCommitName();
        $queryBuilder = $conn->createQueryBuilder();
        $query = $queryBuilder
            ->select('version')
            ->from($commitName)
            ->andWhere('aggregate_identifier = :aggregate_identifier')
            ->orderBy('version', 'DESC')
            ->setMaxResults(1)
            ->setParameter('aggregate_identifier', $identifier);

        $version = (integer)$query->execute()->fetchColumn();
        $this->runtimeVersionCache[$identifier] = $version;
        return $version ?: 1;
    }
}
