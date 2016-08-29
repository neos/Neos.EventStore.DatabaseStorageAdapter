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
use \TYPO3\Flow\Persistence\Doctrine\DataTypes;

/**
 * JsonArrayType
 */
class JsonArrayType extends DataTypes\JsonArrayType
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

    /**
     * @param array $fieldDeclaration
     * @param AbstractPlatform $platform
     * @return string
     */
    public function getSQLDeclaration(array $fieldDeclaration, AbstractPlatform $platform)
    {
        switch ($platform->getName()) {
            case 'postgresql':
                return 'jsonb';
            case 'mysql':
                return 'json';
            default:
                return $platform->getJsonTypeDeclarationSQL($fieldDeclaration);
        }
    }
}
