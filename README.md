# EventStore Database (DBAL) Adapter

_This package is currently under development and not fully working, please don't use it in production._

Check for more documentation:

- [Neos.Cqrs](https://github.com/neos/Neos.Cqrs)
- [Neos.EventStore](https://github.com/neos/Neos.EventStore)

## Installation

    composer require neos/eventstore-databasestorageadapter dev-master

Please refer to this package's ```Settings.yaml``` for configuration options for the database connection. The Event
Store Storage does not use the main Flow persistence connection for performance reasons (different databases between
the Event Store and the projections can help tune performance issues and simplify administrative task like backup, ...).

    ./flow eventstore:createschema

## Drop the tables

    ./flow eventstore:dropschema

License
-------

Licensed under MIT, see [LICENSE](LICENSE)
