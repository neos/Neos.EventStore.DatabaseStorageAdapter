<?php
namespace Flowpack\EventStore\DatabaseStorageAdapter\Schema;

/*
 * This file is part of the Flowpack.Cqrs package.
 *
 * (c) Hand crafted with love in each details by medialib.tv
 */

use Doctrine\DBAL\Schema\Schema;

/**
 * Use this helper in a doctrine migrations script to set up the event store schema
 */
final class EventStoreSchema
{
    /**
     * Use this method when you work with a single stream strategy
     *
     * @param Schema $schema
     * @param string $streamName Defaults to 'event_stream'
     */
    public static function createStream(Schema $schema, $streamName = 'event_stream')
    {
        $eventStream = $schema->createTable($streamName);

        // UUID4 of the event
        $eventStream->addColumn('identifier', 'string', ['fixed' => true, 'length' => 36]);

        // Version of the aggregate after event was recorded
        $eventStream->addColumn('version', 'bigint', ['unsigned' => true]);

        // Name of the event
        $eventStream->addColumn('name', 'string', ['length' => 1000]);
        $eventStream->addColumn('name_hash', 'string', ['length' => 32]);

        // Event payload
        $eventStream->addColumn('payload', 'json_array');
        $eventStream->addColumn('payload_hash', 'string', ['length' => 32]);

        // DateTime ISO8601 + microseconds UTC stored as a string e.g. 2016-02-02T11:45:39.000000
        $eventStream->addColumn('timestamp', 'string', ['fixed' => true, 'length' => 26]);

        // UUID4 of linked aggregate
        $eventStream->addColumn('aggregate_identifier', 'string', ['fixed' => true, 'length' => 36]);

        // Class of the linked aggregate
        $eventStream->addColumn('aggregate_name', 'string', ['length' => 1000]);
        $eventStream->addColumn('aggregate_name_hash', 'string', ['length' => 32]);

        $eventStream->setPrimaryKey(['identifier']);

        // Concurrency check on database level
        $eventStream->addUniqueIndex(['aggregate_identifier', 'version'], $streamName . '_m_v_uix');

        $eventStream->addIndex(['aggregate_identifier'], $streamName . '_ai');

        $eventStream->addIndex(['payload_hash'], $streamName . '_ph');
        $eventStream->addIndex(['aggregate_name_hash'], $streamName . '_anh');
    }

    /**
     * Drop a stream schema
     *
     * @param Schema $schema
     * @param string $streamName Defaults to 'event_stream'
     */
    public static function dropStream(Schema $schema, $streamName = 'event_stream')
    {
        $schema->dropTable($streamName);
    }
}
