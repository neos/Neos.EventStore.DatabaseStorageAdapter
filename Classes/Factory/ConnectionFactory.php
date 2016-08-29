<?php
namespace Ttree\EventStore\DatabaseStorageAdapter\Factory;

/*
 * This file is part of the Ttree.Cqrs package.
 *
 * (c) Hand crafted with love in each details by medialib.tv
 */

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Types\Type;
use TYPO3\Flow\Annotations as Flow;

/**
 * ConnectionFactory
 *
 * @Flow\Scope("singleton")
 */
class ConnectionFactory
{
    /**
     * @var array
     * @Flow\InjectConfiguration(path="persistence")
     */
    protected $configuration;

    /**
     * @var Connection
     */
    protected $connection;

    /**
     * @return Connection
     */
    public function get()
    {
        if ($this->connection !== null) {
            return $this->connection;
        }
        $config = new Configuration();
        $connectionParams = $this->configuration['backendOptions'];
        $this->connection = DriverManager::getConnection($connectionParams, $config);

        if (isset($this->configuration['mappingTypes']) && is_array($this->configuration['mappingTypes'])) {
            foreach ($this->configuration['mappingTypes'] as $typeName => $typeConfiguration) {
                Type::addType($typeName, $typeConfiguration['className']);
                $this->connection->getDatabasePlatform()->registerDoctrineTypeMapping($typeConfiguration['dbType'], $typeName);
            }
        }
        
        return $this->connection;
    }

    /**
     * @return string
     */
    public function getCommitName()
    {
        return $this->configuration['name']['commit'];
    }

    /**
     * @return string
     */
    public function getStreamName()
    {
        return $this->configuration['name']['stream'];
    }
}
