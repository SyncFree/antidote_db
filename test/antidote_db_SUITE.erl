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
-module(antidote_db_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("antidote_utils/include/antidote_utils.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([wrong_types_passed_in/1,
    leveldb_get_snapshot/1,
    leveldb_put_snapshot/1,
    leveldb_get_op/1,
    leveldb_put_op/1]).

all() -> [wrong_types_passed_in, leveldb_get_snapshot, leveldb_put_snapshot, leveldb_get_op, leveldb_put_op].

wrong_types_passed_in(_Config) ->
    ?assertEqual({error, type_not_supported}, antidote_db:new("TEST", type)),
    ?assertEqual({error, type_not_supported}, antidote_db:close_and_destroy({type, db}, name)),
    ?assertEqual({error, type_not_supported}, antidote_db:close({type, db})),
    ?assertEqual({error, type_not_supported}, antidote_db:get_snapshot({type, db}, key, [])),
    ?assertEqual({error, type_not_supported}, antidote_db:put_snapshot({type, db}, key, [])),
    ?assertEqual({error, type_not_supported}, antidote_db:get_ops({type, db}, key, [], [])),
    ?assertEqual({error, type_not_supported}, antidote_db:put_op({type, db}, key, [], [])).

%% This tests stopped working in Erlang 18+ because of a problem with Meck and eleveldb dependency
%%
%%leveldb_new(_Config) ->
%%    Name = "test_db",
%%    Type = leveldb,
%%    Ref = ref,
%%    meck:new(eleveldb),
%%    meck:expect(eleveldb, open, fun(_Name, _Op) -> {ok, Ref} end),
%%    ?assertEqual({ok, {leveldb, Ref}}, antidote_db:new(Name, Type)),
%%    ?assert(meck:validate(eleveldb)),
%%    meck:unload(eleveldb).
%%
%%leveldb_close_and_destroy(_Config) ->
%%    DB = ref,
%%    AntidoteDB = {leveldb, DB},
%%    Name = "test_db",
%%    meck:new(eleveldb),
%%    meck:expect(eleveldb, close, fun(_DB) -> ok end),
%%    meck:expect(eleveldb, destroy, fun(_DB, _Name) -> ok end),
%%    ?assertEqual(ok, antidote_db:close_and_destroy(AntidoteDB, Name)),
%%    ?assert(meck:validate(eleveldb)),
%%    meck:unload(eleveldb).
%%
%%leveldb_close(_Config) ->
%%    DB = ref,
%%    AntidoteDB = {leveldb, DB},
%%    meck:new(eleveldb),
%%    meck:expect(eleveldb, close, fun(_DB) -> ok end),
%%    ?assertEqual(ok, antidote_db:close(AntidoteDB)),
%%    ?assert(meck:validate(eleveldb)),
%%    meck:unload(eleveldb).

leveldb_get_snapshot(_Config) ->
    DB = ref,
    AntidoteDB = {leveldb, DB},
    Key = key,
    CommitTime = ct,
    Snapshot = {ok, snapshot},
    meck:new(leveldb_wrapper),
    meck:expect(leveldb_wrapper, get_snapshot, fun(_DB, _Key, _CommitTime) -> Snapshot end),
    ?assertEqual(Snapshot, antidote_db:get_snapshot(AntidoteDB, Key, CommitTime)),
    ?assert(meck:validate(leveldb_wrapper)),
    meck:unload(leveldb_wrapper).

leveldb_put_snapshot(_Config) ->
    DB = ref,
    AntidoteDB = {leveldb, DB},
    Key = key,
    Snapshot = snapshot,
    meck:new(leveldb_wrapper),
    meck:expect(leveldb_wrapper, put_snapshot, fun(_DB, _Key, _Snapshot) -> ok end),
    ?assertEqual(ok, antidote_db:put_snapshot(AntidoteDB, Key, Snapshot)),
    ?assert(meck:validate(leveldb_wrapper)),
    meck:unload(leveldb_wrapper).

leveldb_get_op(_Config) ->
    DB = ref,
    AntidoteDB = {leveldb, DB},
    Key = key,
    CommitTime = ct,
    OPS = [op1, op2],
    meck:new(leveldb_wrapper),
    meck:expect(leveldb_wrapper, get_ops, fun(_DB, _Key, _CommitTime, _CommitTime) -> OPS end),
    ?assertEqual(OPS, antidote_db:get_ops(AntidoteDB, Key, CommitTime, CommitTime)),
    ?assert(meck:validate(leveldb_wrapper)),
    meck:unload(leveldb_wrapper).

leveldb_put_op(_Config) ->
    DB = ref,
    AntidoteDB = {leveldb, DB},
    Key = key,
    CommitTime = ct,
    OP = op,
    meck:new(leveldb_wrapper),
    meck:expect(leveldb_wrapper, put_op, fun(_DB, _Key, _CommitTime, _OP) -> ok end),
    ?assertEqual(ok, antidote_db:put_op(AntidoteDB, Key, CommitTime, OP)),
    ?assert(meck:validate(leveldb_wrapper)),
    meck:unload(leveldb_wrapper).