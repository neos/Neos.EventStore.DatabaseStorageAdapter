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
use Neos\Cqrs\Event\EventTransport;
use Neos\EventStore\DatabaseStorageAdapter\Factory\ConnectionFactory;
use Neos\EventStore\DatabaseStorageAdapter\Persistence\Doctrine\DataTypes\DateTimeType;
use Neos\EventStore\EventStreamData;
use Neos\EventStore\Exception\ConcurrencyException;
use Neos\EventStore\Exception\StorageConcurrencyException;
use Neos\EventStore\Serializer\JsonSerializer;
use Neos\EventStore\Storage\EventStorageInterface;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Property\PropertyMappingConfiguration;
use TYPO3\Flow\Property\TypeConverter\ObjectConverter;
use TYPO3\Flow\Utility\TypeHandling;

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
            ->select('type, payload, metadata')
            ->from($this->connectionFactory->getStreamTableName())
            ->andWhere('stream_hash = :stream_hash')
            ->orderBy('number', 'ASC')
            ->setParameter('stream_hash', md5($streamName));

        $data = $this->unserializeEvents($query);

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
     * @param int $expectedVersion
     * @param \Closure $callback
     * @return int
     * @throws \Exception
     */
    public function commit(string $streamName, array $data, int $expectedVersion, \Closure $callback = null)
    {
        $stream = new EventStreamData($data, $expectedVersion);
        $connection = $this->connectionFactory->get();
        if ($callback !== null) {
            $connection->beginTransaction();
        }

        $queryBuilder = $connection->createQueryBuilder();

        $now = Timestamp::create();

        $query = $queryBuilder
            ->insert($this->connectionFactory->getStreamTableName())
            ->values([
                'stream' => ':stream',
                'stream_hash' => ':stream_hash',
                'number' => ':number',
                'type' => ':type',
                'type_hash' => ':type_hash',
                'payload' => ':payload',
                'metadata' => ':metadata',
                'savedat' => ':savedat'
            ])
            ->setParameters([
                'stream' => $streamName,
                'stream_hash' => md5($streamName),
                'savedat' => $now,
            ], [
                'stream' => \PDO::PARAM_STR,
                'stream_hash' => \PDO::PARAM_STR,
                'version' => \PDO::PARAM_INT,
                'type' => \PDO::PARAM_STR,
                'payload' => \PDO::PARAM_STR,
                'metadata' => \PDO::PARAM_STR,
                'savedat' => DateTimeType::DATETIME_MICRO,
            ]);

        $version = $this->getCurrentVersion($streamName);
        array_map(function (EventTransport $eventTransport) use ($query, &$version) {
            $version++;
            $event = $this->serializer->serialize($eventTransport->getEvent());
            $metadata = $this->serializer->serialize($eventTransport->getMetaData());
            $type = TypeHandling::getTypeForValue($eventTransport->getEvent());
            $query->setParameter('number', $version);
            $query->setParameter('type', $type);
            $query->setParameter('type_hash', md5($type));
            $query->setParameter('payload', $event);
            $query->setParameter('metadata', $metadata);
            $query->execute();
        }, $stream->getData());

        if ($version !== $expectedVersion) {
            throw new ConcurrencyException(sprintf('Version %d is not egal to expected version %d', $version, $expectedVersion), 1474663323);
        }

        if ($callback !== null) {
            try {
                $callback($expectedVersion);
                $connection->commit();
            } catch (\Exception $exception) {
                $connection->rollBack();
                throw $exception;
            }
        }

        return $expectedVersion;
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
            ->select('number')
            ->from($this->connectionFactory->getStreamTableName())
            ->andWhere('stream_hash = :stream_hash')
            ->orderBy('number', 'DESC')
            ->setMaxResults(1)
            ->setParameter('stream_hash', md5($streamName));

        $version = (integer)$query->execute()->fetchColumn();
        return $version ?: 0;
    }

    /**
     * @param QueryBuilder $query
     * @return array
     */
    protected function unserializeEvents(QueryBuilder $query): array
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
                $this->serializer->unserialize($stream['payload']),
                $this->serializer->unserialize($stream['metadata'])
            );
        }
        return $data;
    }
}
