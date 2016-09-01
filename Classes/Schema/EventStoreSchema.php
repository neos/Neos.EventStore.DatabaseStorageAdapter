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
    public static function createCommit(Schema $schema, string $name)
    {
        $table = $schema->createTable($name);

        // UUID4 of the commit
        $table->addColumn('identifier', Type::STRING, ['fixed' => true, 'length' => 36]);

        // Version of the aggregate after event was recorded
        $table->addColumn('version', Type::BIGINT, ['unsigned' => true]);

        // Events of the commit
        $table->addColumn('data', Type::TEXT);
        $table->addColumn('data_hash', Type::STRING, ['length' => 32]);

        // Timestamp of the commit
        $table->addColumn('created_at', DateTimeType::DATETIME_MICRO);

        // UUID4 of linked aggregate
        $table->addColumn('aggregate_identifier', Type::TEXT);
        $table->addColumn('aggregate_identifier_hash', Type::STRING, ['length' => 32]);

        // Class of the linked aggregate
        $table->addColumn('aggregate_name', Type::STRING, ['length' => 1000]);
        $table->addColumn('aggregate_name_hash', Type::STRING, ['length' => 32]);

        $table->setPrimaryKey(['identifier']);

        // Concurrency check on database level
        $table->addUniqueIndex(['aggregate_identifier_hash', 'version'], $name . '_v_uix');

        $table->addIndex(['data_hash'], $name . '_dh');
        $table->addIndex(['aggregate_name_hash'], $name . '_anh');
    }

    /**
     * Use this method when you work with a single stream strategy
     *
     * @param Schema $schema
     * @param string $name
     */
    public static function createStream(Schema $schema, string $name)
    {
        $table = $schema->createTable($name);

        // UUID4 of the event
        $table->addColumn('identifier', Type::STRING, ['fixed' => true, 'length' => 36]);

        // Commit version
        $table->addColumn('commit_version', Type::BIGINT, ['unsigned' => true]);

        // Version of the event
        $table->addColumn('version', Type::BIGINT, ['unsigned' => true]);

        // Name of the event
        $table->addColumn('type', Type::STRING, ['length' => 1000]);
        $table->addColumn('type_hash', Type::STRING, ['length' => 32]);

        // Event payload
        $table->addColumn('properties', Type::TEXT);

        // Timestamp of the event
        $table->addColumn('created_at', DateTimeType::DATETIME_MICRO);

        // UUID4 of linked aggregate
        $table->addColumn('aggregate_identifier', Type::TEXT);
        $table->addColumn('aggregate_identifier_hash', Type::STRING, ['length' => 32]);

        // Class of the linked aggregate
        $table->addColumn('aggregate_name', Type::STRING, ['length' => 1000]);
        $table->addColumn('aggregate_name_hash', Type::STRING, ['length' => 32]);

        // Concurrency check on database level
        $table->setPrimaryKey(['identifier', 'version'], $name . '_v_uix');

        $table->addIndex(['aggregate_identifier_hash', 'commit_version'], $name . '_ai_cv');

        $table->addIndex(['aggregate_name_hash'], $name . '_anh');
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
