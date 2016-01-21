# AntidoteDB  

Erlang Rebar application, used as the DataStore for [Antidote](https://github.com/SyncFree/antidote). This module exposes a simple API, so the datastore can be configurable and changed, without affecting Antidote code. 

## ElevelDB  

Today, this application uses SyncFree [ElevelDB](https://github.com/SyncFree/eleveldb) fork.

As this application is intended to be used in [Antidote](https://github.com/SyncFree/antidote), it uses the 'Antidote Comparator' by default. 

