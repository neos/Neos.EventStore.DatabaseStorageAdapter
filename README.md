# EventStore Database (DBAL) Adapter

_This package is currently under development and not fully working, please don't use it in production._

Check for more documentation:
 
- [Neos.Cqrs](https://github.com/neos/Neos.Cqrs)
- [Neos.EventStore](https://github.com/neos/Neos.EventStore)

## Installation

    composer require neos/eventstore-databasestorageadapter dev-master

Check ```Settings.yaml``` to configure the database connection used by the event storage. The event storage dont use the
main Flow persistence connection for tunning reasons (different database between the event store and the read projections
can help tune performance issue and administrative task like backup, ...).

    flow eventstore:createschema
    
## Drop the tables

    flow eventstore:dropschema
    
License
-------

Licensed under MIT, see [LICENSE](LICENSE)
