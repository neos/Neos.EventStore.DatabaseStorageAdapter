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
 * DateTimeType
 */
class DateTimeType extends Type
{
    const DATETIME_MICRO = 'datetime_micro';
    const MICRO_FORMAT = 'Y-m-d H:i:s.u';

    /**
     * {@inheritdoc}
     */
    public function getName()
    {
        return self::DATETIME_MICRO;
    }

    /**
     * {@inheritdoc}
     */
    public function getSQLDeclaration(array $fieldDeclaration, AbstractPlatform $platform)
    {
        $declaration = 'DATETIME(6)';
        $declaration .= ' COMMENT \'(DC2Type:' . self::DATETIME_MICRO . ')\'';
        return $declaration;
    }

    /**
     * {@inheritdoc}
     */
    public function convertToPHPValue($value, AbstractPlatform $platform)
    {
        if (null === $value || $value instanceof \DateTime) {
            return $value;
        }
        $converted = \DateTimeImmutable::createFromFormat(self::MICRO_FORMAT, $value);
        if (!$converted) {
            throw ConversionException::conversionFailedFormat(
                $value,
                $this->getName(),
                self::MICRO_FORMAT
            );
        }
        return $converted;
    }

    /**
     * {@inheritdoc}
     */
    public function convertToDatabaseValue($value, AbstractPlatform $platform)
    {
        if (null === $value) {
            return $value;
        }
        if (!($value instanceof \DateTimeInterface)) {
            throw ConversionException::conversionFailedFormat(
                $value,
                $this->getName(),
                self::MICRO_FORMAT
            );
        }
        return $value->format(self::MICRO_FORMAT);
    }
}
