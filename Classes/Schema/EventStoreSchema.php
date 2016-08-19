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
    public static function createStream(Schema $schema, string $name)
    {
        $table = $schema->createTable($name);

        // UUID4 of the event
        $table->addColumn('identifier', Type::STRING, ['fixed' => true, 'length' => 36]);

        // Version of the aggregate after event was recorded
        $table->addColumn('version', Type::BIGINT, ['unsigned' => true]);

        // Name of the event
        $table->addColumn('name', Type::STRING, ['length' => 1000]);
        $table->addColumn('name_hash', Type::STRING, ['length' => 32]);

        // Event payload
        $table->addColumn('payload', Type::JSON_ARRAY);
        $table->addColumn('payload_hash', Type::STRING, ['length' => 32]);

        // DateTime ISO8601 + microseconds UTC stored as a string e.g. 2016-02-02T11:45:39.000000
        $table->addColumn('timestamp', Type::DATETIME);
        $table->addColumn('microseconds', Type::INTEGER, ['unsigned' => true]);

        // UUID4 of linked aggregate
        $table->addColumn('aggregate_identifier', Type::STRING, ['fixed' => true, 'length' => 36]);

        // Class of the linked aggregate
        $table->addColumn('aggregate_name', Type::STRING, ['length' => 1000]);
        $table->addColumn('aggregate_name_hash', Type::STRING, ['length' => 32]);

        $table->setPrimaryKey(['identifier']);

        // Concurrency check on database level
        $table->addUniqueIndex(['aggregate_identifier', 'version'], $name . '_m_v_uix');

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
