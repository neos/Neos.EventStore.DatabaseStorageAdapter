<?php
namespace Neos\EventStore\DatabaseStorageAdapter;

/*
 * This file is part of the Neos.EventStore.DatabaseStorageAdapter package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Query\QueryBuilder;
use Neos\Cqrs\Domain\Timestamp;
use Neos\Cqrs\Event\EventTransport;
use Neos\Cqrs\Event\EventType;
use Neos\EventStore\DatabaseStorageAdapter\Factory\ConnectionFactory;
use Neos\EventStore\DatabaseStorageAdapter\Persistence\Doctrine\DataTypes\DateTimeType;
use Neos\EventStore\EventStream;
use Neos\EventStore\EventStreamData;
use Neos\EventStore\Exception\AggregateNotFoundException;
use Neos\EventStore\Exception\StorageConcurrencyException;
use Neos\EventStore\Storage\EventStorageInterface;
use Neos\EventStore\Storage\PreviousEventsInterface;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Property\PropertyMapper;
use TYPO3\Flow\Utility\Algorithms;

/**
 * Database event storage, for testing purpose
 */
class DatabaseEventStorage implements EventStorageInterface, PreviousEventsInterface
{
    /**
     * @var ConnectionFactory
     * @Flow\Inject
     */
    protected $connectionFactory;

    /**
     * @var PropertyMapper
     * @Flow\Inject
     */
    protected $propertyMapper;

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
            ->select('data, aggregate_name')
            ->from($commitName)
            ->andWhere('aggregate_identifier_hash = :aggregate_identifier_hash')
            ->orderBy('version', 'ASC')
            ->setParameter('aggregate_identifier_hash', md5($identifier));

        list($aggregateName, $data) = $this->eventStreamFromCommitQuery($query);

        if ($aggregateName === null) {
            return null;
        }

        $cacheKey = md5($identifier . '.' . $version);
        $this->runtimeCache[$cacheKey] = new EventStreamData($identifier, $aggregateName, $data, $version);

        return $this->runtimeCache[$cacheKey];
    }

    /**
     * @param string $aggregateIdentifier
     * @param string $aggregateName
     * @param array $data
     * @param integer $version
     * @throws StorageConcurrencyException
     */
    public function commit(string $aggregateIdentifier, string $aggregateName, array $data, int $version)
    {
        $stream = new EventStreamData($aggregateIdentifier, $aggregateName, $data, $version);
        $conn = $this->connectionFactory->get();

        $commitName = $this->connectionFactory->getCommitName();

        $queryBuilder = $conn->createQueryBuilder();

        $streamData = array_map(function (EventTransport $eventTransport) {
            return $this->propertyMapper->convert($eventTransport, 'array');
        }, $stream->getData());

        $now = Timestamp::create();

        $streamData = json_encode($streamData, JSON_PRETTY_PRINT);
        $commitIdentifier = Algorithms::generateUUID();
        $query = $queryBuilder
            ->insert($commitName)
            ->values([
                'identifier' => ':identifier',
                'version' => ':version',
                'data' => ':data',
                'data_hash' => ':data_hash',
                'created_at' => ':created_at',
                'aggregate_identifier' => ':aggregate_identifier',
                'aggregate_identifier_hash' => ':aggregate_identifier_hash',
                'aggregate_name' => ':aggregate_name',
                'aggregate_name_hash' => ':aggregate_name_hash'
            ])
            ->setParameters([
                'identifier' => $commitIdentifier,
                'version' => $version,
                'data' => $streamData,
                'data_hash' => md5($streamData),
                'created_at' => $now,
                'aggregate_identifier' => $aggregateIdentifier,
                'aggregate_identifier_hash' => md5($aggregateIdentifier),
                'aggregate_name' => $aggregateName,
                'aggregate_name_hash' => md5($aggregateName)
            ], [
                'identifier' => \PDO::PARAM_STR,
                'version' => \PDO::PARAM_INT,
                'data' => \PDO::PARAM_STR,
                'data_hash' => \PDO::PARAM_STR,
                'created_at' => DateTimeType::DATETIME_MICRO,
                'aggregate_identifier' => \PDO::PARAM_STR,
                'aggregate_identifier_hash' => \PDO::PARAM_STR,
                'aggregate_name' => \PDO::PARAM_STR,
                'aggregate_name_hash' => \PDO::PARAM_STR
            ]);

        try {
            $query->execute();
        } catch (UniqueConstraintViolationException $exception) {
            throw new StorageConcurrencyException(
                sprintf(
                    'Aggregate root versions mismatch, storage exception, version %d',
                    $version
                ), [], 1472296099
            );
        }

        $this->commitStream($commitIdentifier, $version, $aggregateIdentifier, $aggregateName, $stream);
    }

    /**
     * @param string $commitIdentifier
     * @param integer $commitVersion
     * @param string $aggregateIdentifier
     * @param string $aggregateName
     * @param EventStreamData $streamData
     */
    protected function commitStream(string $commitIdentifier, int $commitVersion, string $aggregateIdentifier, string $aggregateName, EventStreamData $streamData)
    {
        $conn = $this->connectionFactory->get();
        $queryBuilder = $conn->createQueryBuilder();
        $streamName = $this->connectionFactory->getStreamName();
        /** @var EventTransport $eventTransport */
        foreach ($streamData->getData() as $version => $eventTransport) {
            $event = $eventTransport->getEvent();
            $properties = $this->propertyMapper->convert($eventTransport->getEvent(), 'array');
            $properties = json_encode($properties, JSON_PRETTY_PRINT);
            $timestamp = $eventTransport->getTimestamp();
            $name = EventType::get($event);
            $query = $queryBuilder
                ->insert($streamName)
                ->values([
                    'identifier' => ':identifier',
                    'commit_version' => ':commit_version',
                    'version' => ':version',
                    'type' => ':type',
                    'type_hash' => ':type_hash',
                    'properties' => ':properties',
                    'created_at' => ':created_at',
                    'aggregate_identifier' => ':aggregate_identifier',
                    'aggregate_identifier_hash' => ':aggregate_identifier_hash',
                    'aggregate_name' => ':aggregate_name',
                    'aggregate_name_hash' => ':aggregate_name_hash'
                ])
                ->setParameters([
                    'identifier' => $commitIdentifier,
                    'commit_version' => $commitVersion,
                    'version' => $version + 1,
                    'type' => $name,
                    'type_hash' => md5($name),
                    'properties' => $properties,
                    'created_at' => $timestamp,
                    'aggregate_identifier' => $aggregateIdentifier,
                    'aggregate_identifier_hash' => md5($aggregateIdentifier),
                    'aggregate_name' => $aggregateName,
                    'aggregate_name_hash' => md5($aggregateName)
                ], [
                    'identifier' => \PDO::PARAM_STR,
                    'commit_version' => \PDO::PARAM_STR,
                    'version' => \PDO::PARAM_INT,
                    'type' => \PDO::PARAM_STR,
                    'type_hash' => \PDO::PARAM_STR,
                    'properties' => \PDO::PARAM_STR,
                    'created_at' => DateTimeType::DATETIME_MICRO,
                    'aggregate_identifier' => \PDO::PARAM_STR,
                    'aggregate_identifier_hash' => \PDO::PARAM_STR,
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
        return $version ?: 0;
    }

    /**
     * @param string $identifier
     * @param integer $untilVersion
     * @return EventStream
     * @throws AggregateNotFoundException
     */
    public function getPreviousEvents(string $identifier, int $untilVersion): EventStream
    {
        $conn = $this->connectionFactory->get();
        $commitName = $this->connectionFactory->getCommitName();
        $queryBuilder = $conn->createQueryBuilder();
        $query = $queryBuilder
            ->select('data, aggregate_name')
            ->from($commitName)
            ->andWhere('aggregate_identifier = :aggregate_identifier AND version >= :untilVersion')
            ->orderBy('version', 'DESC')
            ->setParameter('aggregate_identifier', $identifier)
            ->setParameter('untilVersion', $untilVersion);

        list($aggregateName, $data) = $this->eventStreamFromCommitQuery($query);

        if ($aggregateName === null) {
            throw new AggregateNotFoundException(sprintf('Aggregate with identifier %s, version=%d, not found', $identifier, $untilVersion), 1472306367);
        }

        return new EventStream($identifier, $aggregateName, $data, $untilVersion);
    }

    /**
     * @param QueryBuilder $query
     * @return array
     */
    protected function eventStreamFromCommitQuery(QueryBuilder $query): array
    {
        $aggregateName = null;
        $data = [];
        foreach ($query->execute()->fetchAll() as $commit) {
            $aggregateName = $commit['aggregate_name'];
            $data = array_merge($data, array_map(function (array $eventData) {
                return $this->propertyMapper->convert($eventData, EventTransport::class);
            }, json_decode($commit['data'], true)));
        }
        if ($aggregateName === null) {
            return [null, $data];
        }
        return [$aggregateName, $data];
    }
}
