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

use Doctrine\DBAL\Query\QueryBuilder;
use Neos\Cqrs\Domain\Timestamp;
use Neos\Cqrs\Event\EventInterface;
use Neos\Cqrs\Event\EventTransport;
use Neos\Cqrs\Message\MessageMetadata;
use Neos\EventStore\DatabaseStorageAdapter\Factory\ConnectionFactory;
use Neos\EventStore\DatabaseStorageAdapter\Persistence\Doctrine\DataTypes\DateTimeType;
use Neos\EventStore\EventStreamData;
use Neos\EventStore\Exception\StorageConcurrencyException;
use Neos\EventStore\Serializer\JsonSerializer;
use Neos\EventStore\Storage\EventStorageInterface;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Property\PropertyMappingConfiguration;
use TYPO3\Flow\Property\TypeConverter\ObjectConverter;

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
     * @var JsonSerializer
     * @Flow\Inject
     */
    protected $serializer;

    /**
     * @var array
     */
    protected $runtimeCache = [];

    /**
     * @param string $streamName
     * @return EventStreamData
     */
    public function load(string $streamName)
    {
        $version = $this->getCurrentVersion($streamName);
        $cacheKey = md5($streamName . '.' . $version);
        if (isset($this->runtimeCache[$cacheKey])) {
            return $this->runtimeCache[$cacheKey];
        }
        $conn = $this->connectionFactory->get();
        $queryBuilder = $conn->createQueryBuilder();
        $query = $queryBuilder
            ->select('event, metadata')
            ->from($this->connectionFactory->getStreamTableName())
            ->andWhere('stream_name_hash = :stream_name_hash')
            ->orderBy('commit_version', 'ASC')
            ->addOrderBy('event_version', 'ASC')
            ->setParameter('stream_name_hash', md5($streamName));

        $data = $this->eventStreamFromCommitQuery($query);

        if ($data === []) {
            return null;
        }

        $cacheKey = md5($streamName . '.' . $version);
        $this->runtimeCache[$cacheKey] = new EventStreamData($data, $version);

        return $this->runtimeCache[$cacheKey];
    }

    /**
     * @param string $streamName
     * @param array $data
     * @param int $commitVersion
     * @return int
     * @throws StorageConcurrencyException
     */
    public function commit(string $streamName, array $data, int $commitVersion)
    {
        $stream = new EventStreamData($data, $commitVersion);
        $conn = $this->connectionFactory->get();

        $queryBuilder = $conn->createQueryBuilder();

        $now = Timestamp::create();

        $query = $queryBuilder
            ->insert($this->connectionFactory->getStreamTableName())
            ->values([
                'stream_name' => ':stream_name',
                'stream_name_hash' => ':stream_name_hash',
                'commit_version' => ':commit_version',
                'event_version' => ':event_version',
                'event' => ':event',
                'metadata' => ':metadata',
                'recorded_at' => ':recorded_at'
            ])
            ->setParameters([
                'stream_name' => $streamName,
                'stream_name_hash' => md5($streamName),
                'commit_version' => $commitVersion,
                'recorded_at' => $now,
            ], [
                'stream_name' => \PDO::PARAM_STR,
                'stream_name_hash' => \PDO::PARAM_STR,
                'version' => \PDO::PARAM_INT,
                'event' => \PDO::PARAM_STR,
                'metadata' => \PDO::PARAM_STR,
                'recorded_at' => DateTimeType::DATETIME_MICRO,
            ]);

        $version = 1;
        array_map(function (EventTransport $eventTransport) use ($query, &$version) {
            $event = $this->serializer->serialize($eventTransport->getEvent());
            $metadata = $this->serializer->serialize($eventTransport->getMetaData());
            $query->setParameter('event_version', $version);
            $query->setParameter('event', $event);
            $query->setParameter('metadata', $metadata);
            $query->execute();
            $version++;
        }, $stream->getData());

        return $commitVersion;
    }

    /**
     * @param string $streamName
     * @return boolean
     */
    public function contains(string $streamName): bool
    {
        return $this->getCurrentVersion($streamName) > 1 ? true : false;
    }

    /**
     * @param  string $streamName
     * @return integer Current Aggregate Root version
     */
    public function getCurrentVersion(string $streamName): int
    {
        $conn = $this->connectionFactory->get();
        $queryBuilder = $conn->createQueryBuilder();
        $query = $queryBuilder
            ->select('commit_version')
            ->from($this->connectionFactory->getStreamTableName())
            ->andWhere('stream_name_hash = :stream_name_hash')
            ->orderBy('commit_version', 'DESC')
            ->addOrderBy('event_version', 'DESC')
            ->setMaxResults(1)
            ->setParameter('stream_name_hash', md5($streamName));

        $version = (integer)$query->execute()->fetchColumn();
        return $version ?: 0;
    }

    /**
     * @param QueryBuilder $query
     * @return array
     */
    protected function eventStreamFromCommitQuery(QueryBuilder $query): array
    {
        $configuration = new PropertyMappingConfiguration();
        $configuration->allowAllProperties();
        $configuration->setTypeConverterOption(
            ObjectConverter::class,
            ObjectConverter::CONFIGURATION_OVERRIDE_TARGET_TYPE_ALLOWED,
            true
        );

        $data = [];
        foreach ($query->execute()->fetchAll() as $stream) {
            $data[] = new EventTransport(
                $this->serializer->unserialize($stream['event']),
                $this->serializer->unserialize($stream['metadata'])
            );
        }
        return $data;
    }
}
