<?php
namespace Ttree\EventStore\DatabaseStorageAdapter\Command;

/*
 * This file is part of the Neos.EventStore.DatabaseStorageAdapter package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Ttree\EventStore\DatabaseStorageAdapter\Factory\ConnectionFactory;
use Ttree\EventStore\DatabaseStorageAdapter\Schema\EventStoreSchema;
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

        EventStoreSchema::createCommit($toSchema, $this->connectionFactory->getCommitName());
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

        if ($schema->hasTable($this->connectionFactory->getCommitName())) {
            EventStoreSchema::drop($toSchema, $this->connectionFactory->getCommitName());
        }

        if ($schema->hasTable($this->connectionFactory->getStreamName())) {
            EventStoreSchema::drop($toSchema, $this->connectionFactory->getStreamName());
        }

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
