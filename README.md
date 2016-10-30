# AntidoteDB  

Erlang Rebar application, used as the backend DataStore for [Antidote](https://github.com/SyncFree/antidote). This module exposes a simple API, so the datastore can be configurable and changed, without affecting Antidote code. 

This project is structured as a rebar3 project, and today contains two modules:

+ **antidote_db**: contains a basic API to interact with the DB. Today it only contains the methods for a eleveldb backend.
+ **leveldb_wrapper**: contains a wrapper of the API provided by eleveldb, with added code containing Antidote logic.

All tests are under the respective *_SUITE modules, and should be run using `rebar3 ct`.

## ElevelDB  

Today, this application uses SyncFree [ElevelDB](https://github.com/SyncFree/eleveldb) fork.

As this application is intended to be used in [Antidote](https://github.com/SyncFree/antidote), it uses the 'Antidote Comparator' by default. 

### Composition of keys

Keys are composed as follows:

`{antidote_key, max_value_in_vc, vc_hash, op/snap, vc}`

+ **antidote_key**: Same key as in Antidote. 
+ **max_value_in_vc**: The max value for all DCs in the VC.
+ **vc_hash**: A hash of the VC. This is done in order to provide a more performant sorting algorithm, since we don't need to compare all the VC, to find that keys are different. If the hash matches, we also compare the VC, just to make sure it's not a collision. 
+ **op/snap**: an atom indicating if the stored value corresponds to an operation or a snapshot.
+ **vc**: the vc of when the op/snap ocurred.

### Sorting of keys

Keys get sorted first by the Antidote key.
Then we look at the MAX value of it's VC, sorting first the biggest one. We reverse the "natural" order, so we can have the most recent operations first.
If the max value for two keys matches, we sort taking into account the hash value.
For matching hashes, we check the rest of the key, sorting accordingly.

### Testing

As stated before, the modules have their corresponding *_SUITE modules for unit testing. The antidote_db module uses [meck](https://github.com/eproxus/meck) to mock the calls to eleveldb and the leveldb_wrapper. 

In addition to this tests, and thanks to @peterzeller, we have a Proper module to test the get_ops method. This module generates operations by simulating incrementing a
vectorclock and merging them. This tests can be run using "rebar3 proper -m prop_get_ops -n TESTS" where TESTS is the amount of random inputs that Proper should run with.

This two modules have more than a 90% code coverage.
