%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(antidote_db).

-include_lib("antidote_utils/include/antidote_utils.hrl").

-export([
    new/2,
    close_and_destroy/2,
    close/1,
    get_ops_applicable_to_snapshot/3,
    get_snapshot/3,
    put_snapshot/3,
    get_ops/4,
    put_op/4]).

-type antidote_db() :: {leveldb, eleveldb:db_ref()}.

-type antidote_db_type() :: leveldb.

-export_type([antidote_db/0, antidote_db_type/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Given a name, returns a new AntidoteDB (for now, only ElevelDB is supported)
%% OpenOptions are set to use Antidote special comparator in the case of Eleveldb
-spec new(atom(), antidote_db_type()) -> {ok, antidote_db()} | {error, any()}.
new(Name, Type) ->
    case Type of
        leveldb ->
            {ok, Ref} = eleveldb:open(Name, [{create_if_missing, true}, {antidote, true}]),
            {ok, {leveldb, Ref}};
        _ ->
            {error, type_not_supported}
    end.


%% Closes and destroys the given base
-spec close_and_destroy(antidote_db(), atom()) -> ok | {error, any()}.
close_and_destroy({Type, DB}, Name) ->
    case Type of
        leveldb ->
            eleveldb:close(DB),
            eleveldb:destroy(Name, []);
        _ ->
            {error, type_not_supported}
    end.

-spec close(antidote_db()) -> ok | {error, any()}.
close({Type, DB}) ->
    case Type of
        leveldb ->
            eleveldb:close(DB);
        _ ->
            {error, type_not_supported}
    end.

-spec get_ops_applicable_to_snapshot(antidote_db:antidote_db(), key(), vectorclock()) ->
    {ok, #materialized_snapshot{} | not_found, [#log_record{}]}.
get_ops_applicable_to_snapshot({Type, DB}, Key, VectorClock) ->
    case Type of
        leveldb ->
            leveldb_wrapper:get_ops_applicable_to_snapshot(DB, Key, VectorClock);
        _ ->
            {error, type_not_supported}
    end.

%% Gets the most suitable snapshot for Key that has been committed
%% before CommitTime. If its nothing is found, returns {error, not_found}
-spec get_snapshot(antidote_db:antidote_db(), key(),
    snapshot_time()) -> {ok, #materialized_snapshot{}} | {error, not_found}.
get_snapshot({Type, DB}, Key, CommitTime) ->
    case Type of
        leveldb ->
            leveldb_wrapper:get_snapshot(DB, Key, CommitTime);
        _ ->
            {error, type_not_supported}
    end.

%% Saves the snapshot into AntidoteDB
-spec put_snapshot(antidote_db:antidote_db(), key(), #materialized_snapshot{}) -> ok | error.
put_snapshot({Type, DB}, Key, Snapshot) ->
    case Type of
        leveldb ->
            leveldb_wrapper:put_snapshot(DB, Key, Snapshot);
        _ ->
            {error, type_not_supported}
    end.

%% Returns a list of operations that have commit time in the range [VCFrom, VCTo]
-spec get_ops(antidote_db:antidote_db(), key(), vectorclock(), vectorclock()) -> [#log_record{}].
get_ops({Type, DB}, Key, VCFrom, VCTo) ->
    case Type of
        leveldb ->
            leveldb_wrapper:get_ops(DB, Key, VCFrom, VCTo);
        _ ->
            {error, type_not_supported}
    end.


%% Saves the operation into AntidoteDB
-spec put_op(antidote_db:antidote_db(), key(), vectorclock(), #log_record{}) -> ok | error.
put_op({Type, DB}, Key, VC, Record) ->
    case Type of
        leveldb ->
            leveldb_wrapper:put_op(DB, Key, VC, Record);
        _ ->
            {error, type_not_supported}
    end.


-ifdef(TEST).

wrong_types_test() ->
    ?assertEqual({error, type_not_supported}, new("TEST", type)),
    ?assertEqual({error, type_not_supported}, close_and_destroy({type, db}, name)),
    ?assertEqual({error, type_not_supported}, close({type, db})),
    ?assertEqual({error, type_not_supported}, get_snapshot({type, db}, key, [])),
    ?assertEqual({error, type_not_supported}, put_snapshot({type, db}, key, [])),
    ?assertEqual({error, type_not_supported}, get_ops({type, db}, key, [], [])),
    ?assertEqual({error, type_not_supported}, put_op({type, db}, key, [], [])).

-endif.
