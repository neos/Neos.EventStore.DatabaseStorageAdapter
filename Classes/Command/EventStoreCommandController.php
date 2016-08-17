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

        $streamName = $this->connectionFactory->getStreamName();

        if ($schema->hasTable($streamName)) {
            $this->outputLine('<comment>!!</comment> Table %s exist ...', [$streamName]);
            $this->outputLine();
            $this->quit(1);
        }

        $this->outputLine('<info>++</info> Create table %s ...', [$streamName]);

        EventStoreSchema::createStream($schema, $streamName);

        $conn->beginTransaction();
        $statements = $schema->toSql($conn->getDatabasePlatform());
        foreach ($statements as $statement) {
            $this->outputLine('<info>++</info> %s', [$statement]);
            $conn->exec($statement);
        }
        $conn->commit();

        $this->outputLine();
    }
}
