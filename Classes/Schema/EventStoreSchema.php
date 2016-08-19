<?php
namespace Ttree\EventStore\DatabaseStorageAdapter\Schema;

/*
 * This file is part of the Ttree.Cqrs package.
 *
 * (c) Hand crafted with love in each details by medialib.tv
 */

use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Types\Type;

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

        // Version of the aggregate after event was recorded
        $table->addColumn('version', Type::BIGINT, ['unsigned' => true]);

        // Events of the commit
        $table->addColumn('data', Type::TEXT);
        $table->addColumn('data_hash', Type::STRING, ['length' => 32]);

        // Timestamp of the commit
        $table->addColumn('created_at', Type::DATETIME);
        $table->addColumn('created_at_microseconds', Type::INTEGER, ['unsigned' => true]);

        // UUID4 of linked aggregate
        $table->addColumn('aggregate_identifier', Type::STRING, ['fixed' => true, 'length' => 36]);

        // Class of the linked aggregate
        $table->addColumn('aggregate_name', Type::STRING, ['length' => 1000]);
        $table->addColumn('aggregate_name_hash', Type::STRING, ['length' => 32]);

        // Concurrency check on database level
        $table->addUniqueIndex(['version'], $name . '_v_uix');

        $table->addIndex(['aggregate_identifier'], $name . '_ai');

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
        $table->addColumn('name', Type::STRING, ['length' => 1000]);
        $table->addColumn('name_hash', Type::STRING, ['length' => 32]);

        // Event payload
        $table->addColumn('payload', Type::TEXT);
        $table->addColumn('payload_hash', Type::STRING, ['length' => 32]);

        // Timestamp of the event
        $table->addColumn('created_at', Type::DATETIME);
        $table->addColumn('created_at_microseconds', Type::INTEGER, ['unsigned' => true]);

        // UUID4 of linked aggregate
        $table->addColumn('aggregate_identifier', Type::STRING, ['fixed' => true, 'length' => 36]);

        // Class of the linked aggregate
        $table->addColumn('aggregate_name', Type::STRING, ['length' => 1000]);
        $table->addColumn('aggregate_name_hash', Type::STRING, ['length' => 32]);

        $table->setPrimaryKey(['identifier']);

        $table->addIndex(['aggregate_identifier'], $name . '_ai');

        $table->addIndex(['payload_hash'], $name . '_ph');
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
