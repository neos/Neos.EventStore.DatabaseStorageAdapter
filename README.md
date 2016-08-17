# EventStore InMemory Storage Adapter

_This package is currently under development and not fully working, please don't use it in production._

This package is inspired by [Prooph EventStore Doctrine Adapter](https://github.com/prooph/event-store-doctrine-adapter).

This package provide an EventStore implementation for ```Flowpack.EventStore``` based on Doctrine DBAL.

## Installation

    composer require flowpack/eventstore-databasestorageadapter dev-master

Check ```Settings.yaml``` to configure the database connection used by the event storage. The event storage dont use the
main Flow persistence connection for tunning reasons (different database between the event store and the read projections
can help tune performance issue and administrative task like backup, ...).

    flow eventstore:createschema

Acknowledgments
---------------

Development sponsored by [ttree ltd - neos solution provider](http://ttree.ch).

We try our best to craft this package with a lots of love, we are open to sponsoring, support request, ... just contact us.

License
-------

Licensed under MIT, see [LICENSE](LICENSE)
