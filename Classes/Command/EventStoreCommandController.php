<?php
namespace Neos\EventStore\DatabaseStorageAdapter\Command;

/*
 * This file is part of the Neos.EventStore.DatabaseStorageAdapter package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Doctrine\DBAL\Exception\ConnectionException;
use Neos\EventStore\DatabaseStorageAdapter\Factory\ConnectionFactory;
use Neos\EventStore\DatabaseStorageAdapter\Schema\EventStoreSchema;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Cli\CommandController;

/**
 * CLI Command Controller for storage related commands of the Neos Event Store
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
     * @var array
     * @Flow\InjectConfiguration(path="persistence")
     */
    protected $configuration;

    /**
     * Create Event Store database tables
     *
     * This command creates the necessary database tables for the Event Store. It uses the Doctrine connection
     * parameters which were defined for Flow.
     *
     * @return void
     */
    public function createSchemaCommand()
    {
        $this->outputLine('Creating Event Store database tables in database "%s" on host %s connecting with user "%s" ...', [ $this->configuration['backendOptions']['dbname'], $this->configuration['backendOptions']['host'], $this->configuration['backendOptions']['user']]);
        try {
            $connection = $this->connectionFactory->get();

            $schema = $connection->getSchemaManager()->createSchema();
            $toSchema = clone $schema;

            EventStoreSchema::createStream($toSchema, $this->connectionFactory->getStreamTableName());

            $connection->beginTransaction();
            $statements = $schema->getMigrateToSql($toSchema, $connection->getDatabasePlatform());
            foreach ($statements as $statement) {
                $this->outputLine('<info>++</info> %s', [$statement]);
                $connection->exec($statement);
            }
            $connection->commit();

            $this->outputLine();
        } catch (ConnectionException $exception) {
            $this->outputLine('<error>Connection failed</error>');
            $this->outputLine('%s', [ $exception->getMessage() ]);
            $this->quit(1);
        }
    }

    /**
     * Drop Event Store database tables
     *
     * This command <b>deletes all</b> Event Store related database tables! It uses the Doctrine connection
     * parameters which were defined for Flow.
     *
     * @return void
     */
    public function dropSchemaCommand()
    {
        $this->outputLine('<error>Warning</error>');
        $this->outputLine('You are about to drop all Event Store related tables in database "%s" on host %s.', [ $this->configuration['backendOptions']['dbname'], $this->configuration['backendOptions']['host']]);
        if (!$this->output->askConfirmation('Are you sure? ', false)) {
            $this->outputLine('Aborted.');
            $this->quit(0);
        }

        try {
            $connection = $this->connectionFactory->get();

            $schema = $connection->getSchemaManager()->createSchema();
            $toSchema = clone $schema;

            if ($schema->hasTable($this->connectionFactory->getStreamTableName())) {
                EventStoreSchema::drop($toSchema, $this->connectionFactory->getStreamTableName());
            }

            $connection->beginTransaction();
            $statements = $schema->getMigrateToSql($toSchema, $connection->getDatabasePlatform());
            foreach ($statements as $statement) {
                $this->outputLine('<info>++</info> %s', [$statement]);
                $connection->exec($statement);
            }
            $connection->commit();

            $this->outputLine();
        } catch (ConnectionException $exception) {
            $this->outputLine('<error>Connection failed</error>');
            $this->outputLine('%s', [ $exception->getMessage() ]);
            $this->quit(1);
        }
    }
}
