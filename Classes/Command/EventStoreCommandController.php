<?php
namespace Flowpack\EventStore\DatabaseStorageAdapter\Command;

/*
 * This file is part of the Flowpack.Cqrs package.
 *
 * (c) Hand crafted with love in each details by medialib.tv
 */

use Flowpack\EventStore\DatabaseStorageAdapter\Factory\ConnectionFactory;
use Flowpack\EventStore\DatabaseStorageAdapter\Schema\EventStoreSchema;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Cli\CommandController;

/**
 * ConnectionFactory
 *
 * @Flow\Scope("singleton")
 */
class EventStoreCommandController extends CommandController
{
    /**
     * @var ConnectionFactory
     * @Flow\Inject
     */
    protected $connectionFactory;

    /**
     * Create eventstore database tables
     */
    public function createSchemaCommand()
    {
        $this->outputLine();
        $conn = $this->connectionFactory->get();
        $schema = $conn->getSchemaManager()->createSchema();
        $toSchema = clone $schema;

        EventStoreSchema::createStream($toSchema, $this->connectionFactory->getStreamName());

        $conn->beginTransaction();
        $statements = $schema->getMigrateToSql($toSchema, $conn->getDatabasePlatform());
        foreach ($statements as $statement) {
            $this->outputLine('<info>++</info> %s', [$statement]);
            $conn->exec($statement);
        }
        $conn->commit();

        $this->outputLine();
    }

    /**
     * Create eventstore database tables
     */
    public function dropSchemaCommand()
    {
        $this->outputLine();
        $conn = $this->connectionFactory->get();
        $schema = $conn->getSchemaManager()->createSchema();
        $toSchema = clone $schema;

        EventStoreSchema::drop($toSchema, $this->connectionFactory->getStreamName());

        $conn->beginTransaction();
        $statements = $schema->getMigrateToSql($toSchema, $conn->getDatabasePlatform());
        foreach ($statements as $statement) {
            $this->outputLine('<info>++</info> %s', [$statement]);
            $conn->exec($statement);
        }
        $conn->commit();

        $this->outputLine();
    }
}
