<?php
namespace Neos\EventStore\DatabaseStorageAdapter\Schema;

/*
 * This file is part of the Neos.EventStore.DatabaseStorageAdapter package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Types\Type;
use Neos\EventStore\DatabaseStorageAdapter\Persistence\Doctrine\DataTypes\DateTimeType;

/**
 * Use this helper in a doctrine migrations script to set up the event store schema
 */
final class EventStoreSchema
{
    /**
     * Use this method when you work with a single stream strategy
     *
     * @param Schema $schema
     * @param string $name
     */
    public static function createStream(Schema $schema, string $name)
    {
        $table = $schema->createTable($name);

        // Auto increment used for deterministic event ordering
        $table->addColumn('id', Type::BIGINT, ['unsigned' => true])
            ->setAutoincrement(true);

        // Stream name and the MD5 Hash of the stream name
        $table->addColumn('stream', Type::TEXT)
            ->setNotnull(true);
        $table->addColumn('stream_hash', Type::STRING, ['length' => 32])
            ->setNotnull(true);

        // Event number in the current stream
        $table->addColumn('number', Type::BIGINT, ['unsigned' => true]);

        // Event type
        $table->addColumn('type', Type::STRING, ['length' => 1000])
            ->setNotnull(true);
        $table->addColumn('type_hash', Type::STRING, ['length' => 32])
            ->setNotnull(true);

        // Events of the stream
        $table->addColumn('payload', Type::TEXT);
        $table->addColumn('metadata', Type::TEXT);

        // Timestamp of the stream
        $table->addColumn('savedat', DateTimeType::DATETIME_MICRO)
            ->setNotnull(true);

        $table->setPrimaryKey(['id']);
        $table->addUniqueIndex(['stream_hash', 'number'], 'stream_number');
    }

    /**
     * @param Schema $schema
     * @param string $name
     */
    public static function drop(Schema $schema, string $name)
    {
        $schema->dropTable($name);
    }
}
