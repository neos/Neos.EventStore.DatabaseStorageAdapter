<?php
namespace Ttree\EventStore\DatabaseStorageAdapter\Persistence\Doctrine\DataTypes;

/*
 * This file is part of the Ttree.Cqrs package.
 *
 * (c) Hand crafted with love in each details by medialib.tv
 */

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Types\ConversionException;
use Doctrine\DBAL\Types\Type;
use TYPO3\Flow\Annotations as Flow;

/**
 * JsonArrayType
 */
class JsonArrayType extends \TYPO3\Flow\Persistence\Doctrine\DataTypes\JsonArrayType
{
    const CQRS_JSON_ARRAY = 'cqrs_json_array';

    /**
     * Gets the name of this type.
     *
     * @return string
     */
    public function getName()
    {
        return self::CQRS_JSON_ARRAY;
    }
}
