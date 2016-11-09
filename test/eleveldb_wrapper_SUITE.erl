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
-module(eleveldb_wrapper_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("antidote_utils/include/antidote_utils.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, withFreshDb/1]).
-export([get_snapshot_matching_vc/1,
    get_snapshot_not_matching_vc/1,
    get_operations_empty_result/1,
    get_operations_non_empty/1,
    operations_and_snapshots_mixed/1,
    concurrent_ops/1,
    smallest_op_returned/1,
    concurrent_op_lower_than_search_range/1]).

all() -> [get_snapshot_matching_vc, get_snapshot_not_matching_vc, get_operations_empty_result,
    get_operations_non_empty, operations_and_snapshots_mixed, concurrent_ops, smallest_op_returned,
    concurrent_op_lower_than_search_range].

withFreshDb(F) ->
    %% Destroy the test DB to prevent having dirty DBs if a test fails
    eleveldb:destroy("test_db", []),
    {ok, DB} = eleveldb:open("test_db", [{create_if_missing, true}, {antidote, true}]),
    try
        F(DB)
    after
        eleveldb:close(DB),
        eleveldb:destroy("test_db", [])
    end.

get_snapshot_matching_vc(_Config) ->
    withFreshDb(fun(DB) ->

        Key = key,
        put_n_snapshots(DB, Key, 10),

        %% Get some of the snapshots inserted (matching VC)
        {ok, S1} = leveldb_wrapper:get_snapshot(DB, Key, vectorclock:from_list([{local, 1}, {remote, 1}])),
        {ok, S2} = leveldb_wrapper:get_snapshot(DB, Key, vectorclock:from_list([{local, 4}, {remote, 4}])),
        {ok, S3} = leveldb_wrapper:get_snapshot(DB, Key, vectorclock:from_list([{local, 8}, {remote, 8}])),

        ?assertEqual([{local, 1}, {remote, 1}], vectorclock_to_sorted_list(S1#materialized_snapshot.snapshot_time)),
        ?assertEqual(1, S1#materialized_snapshot.value),

        ?assertEqual([{local, 4}, {remote, 4}], vectorclock_to_sorted_list(S2#materialized_snapshot.snapshot_time)),
        ?assertEqual(4, S2#materialized_snapshot.value),

        ?assertEqual([{local, 8}, {remote, 8}], vectorclock_to_sorted_list(S3#materialized_snapshot.snapshot_time)),
        ?assertEqual(8, S3#materialized_snapshot.value)
                end).

get_snapshot_not_matching_vc(_Config) ->
    withFreshDb(fun(DB) ->
        Key = key,

        %% Add 3 snapshots
        VC = vectorclock:from_list([{local, 4}, {remote, 4}]),
        leveldb_wrapper:put_snapshot(DB, Key, #materialized_snapshot{snapshot_time = VC, value = 4}),

        VC1 = vectorclock:from_list([{local, 2}, {remote, 3}]),
        leveldb_wrapper:put_snapshot(DB, Key, #materialized_snapshot{snapshot_time = VC1, value = 2}),

        VC2 = vectorclock:from_list([{local, 8}, {remote, 7}]),
        leveldb_wrapper:put_snapshot(DB, Key, #materialized_snapshot{snapshot_time = VC2, value = 8}),

        %% Request the snapshots using a VC different than the one used to insert them
        S4 = leveldb_wrapper:get_snapshot(DB, Key, vectorclock:from_list([{local, 1}, {remote, 0}])),
        {ok, S5} = leveldb_wrapper:get_snapshot(DB, Key, vectorclock:from_list([{local, 6}, {remote, 5}])),
        {ok, S6} = leveldb_wrapper:get_snapshot(DB, Key, vectorclock:from_list([{local, 8}, {remote, 9}])),

        ?assertEqual({error, not_found}, S4),

        ?assertEqual([{local, 4}, {remote, 4}], vectorclock_to_sorted_list(S5#materialized_snapshot.snapshot_time)),
        ?assertEqual(4, S5#materialized_snapshot.value),

        ?assertEqual([{local, 8}, {remote, 7}], vectorclock_to_sorted_list(S6#materialized_snapshot.snapshot_time)),
        ?assertEqual(8, S6#materialized_snapshot.value)
                end).

get_operations_empty_result(_Config) ->
    withFreshDb(fun(DB) ->
        Key = key,
        Key1 = key1,

        %% Nothing in the DB returns an empty list
        O1 = leveldb_wrapper:get_ops(DB, Key, [{local, 2}, {remote, 2}], [{local, 8}, {remote, 9}]),
        ?assertEqual([], O1),

        %% Insert some operations
        put_n_operations(DB, Key, 10),

        %% Requesting for ops in a range with noting, returns an empty list
        O2 = leveldb_wrapper:get_ops(DB, Key, [{local, 123}, {remote, 100}], [{local, 200}, {remote, 124}]),
        ?assertEqual([], O2),

        %% Getting a key not present, returns an empty list
        O3 = leveldb_wrapper:get_ops(DB, Key1, [{local, 2}, {remote, 2}], [{local, 4}, {remote, 5}]),
        ?assertEqual([], O3),

        %% Searching for the same range returns an empty list
        O4 = leveldb_wrapper:get_ops(DB, Key1, [{local, 2}, {remote, 2}], [{local, 2}, {remote, 2}]),
        ?assertEqual([], O4)
                end).

get_operations_non_empty(_Config) ->
    withFreshDb(fun(DB) ->
        %% Fill the DB with values
        Key = key,
        Key1 = key1,
        Key2 = key2,

        put_n_operations(DB, Key, 100),
        put_n_operations(DB, Key1, 10),
        put_n_operations(DB, Key2, 25),

        O1 = leveldb_wrapper:get_ops(DB, Key1, [{local, 2}, {remote, 2}], [{local, 8}, {remote, 9}]),
        ?assertEqual([3, 4, 5, 6, 7, 8], filter_records_into_sorted_numbers(O1)),

        O2 = leveldb_wrapper:get_ops(DB, Key1, [{local, 4}, {remote, 5}], [{local, 7}, {remote, 7}]),
        ?assertEqual([5, 6, 7], filter_records_into_sorted_numbers(O2))
                end).

operations_and_snapshots_mixed(_Config) ->
    withFreshDb(fun(DB) ->
        Key = key,
        Key1 = key1,
        Key2 = key2,

        VCTo = [{local, 7}, {remote, 8}],
        put_n_operations(DB, Key, 10),
        put_n_operations(DB, Key1, 20),
        leveldb_wrapper:put_snapshot(DB, Key1, #materialized_snapshot{snapshot_time = [{local, 2}, {remote, 3}], value = 5}),
        put_n_operations(DB, Key2, 8),

        %% We want all ops for Key1 that are between the snapshot and
        %% [{local, 7}, {remote, 8}]. First get the snapshot, then OPS.
        {ok, Snapshot} = leveldb_wrapper:get_snapshot(DB, Key1, vectorclock:from_list(VCTo)),
        ?assertEqual([{local, 2}, {remote, 3}], vectorclock_to_sorted_list(Snapshot#materialized_snapshot.snapshot_time)),
        ?assertEqual(5, Snapshot#materialized_snapshot.value),

        O1 = leveldb_wrapper:get_ops(DB, Key1, Snapshot#materialized_snapshot.snapshot_time, VCTo),
        ?assertEqual([3, 4, 5, 6, 7], filter_records_into_sorted_numbers(O1))
                end).

%% This test inserts 5 ops, 4 of them are concurrent, and checks that only the first and two of the concurrent are
%% returned, since they are the only ones that match the requested ranged passed in
concurrent_ops(_Config) ->
    withFreshDb(fun(DB) ->
        ok = leveldb_wrapper:put_op(DB, d, [{dc1, 10}, {dc2, 14}, {dc3, 3}], #log_record{version = 1}),
        ok = leveldb_wrapper:put_op(DB, d, [{dc1, 9}, {dc2, 12}, {dc3, 1}], #log_record{version = 2}),
        ok = leveldb_wrapper:put_op(DB, d, [{dc1, 7}, {dc2, 2}, {dc3, 12}], #log_record{version = 3}),
        ok = leveldb_wrapper:put_op(DB, d, [{dc1, 5}, {dc2, 2}, {dc3, 10}], #log_record{version = 4}),
        ok = leveldb_wrapper:put_op(DB, d, [{dc1, 1}, {dc2, 1}, {dc3, 1}], #log_record{version = 5}),

        OPS = leveldb_wrapper:get_ops(DB, d, [{dc1, 10}, {dc2, 17}, {dc3, 2}], [{dc1, 12}, {dc2, 20}, {dc3, 18}]),

        ?assertEqual([1, 3, 4], filter_records_into_sorted_numbers(OPS))
                end).

smallest_op_returned(_Config) ->
    withFreshDb(fun(DB) ->
        ok = leveldb_wrapper:put_op(DB, key, [{dc3, 1}], #log_record{version = 4}),
        ok = leveldb_wrapper:put_op(DB, key, [{dc2, 4}, {dc3, 1}], #log_record{version = 3}),
        ok = leveldb_wrapper:put_op(DB, key, [{dc2, 2}], #log_record{version = 2}),
        ok = leveldb_wrapper:put_op(DB, key, [{dc2, 1}], #log_record{version = 1}),

        OPS = leveldb_wrapper:get_ops(DB, key, [{dc2, 3}], [{dc2, 3}, {dc3, 1}]),

        ?assertEqual([4], filter_records_into_sorted_numbers(OPS))
                end).

%% This test ensures that the break condition doesn't stop in the min value of DCS
%% if the VCs in the search range, have different keyset.
concurrent_op_lower_than_search_range(_Config) ->
    withFreshDb(fun(DB) ->
        ok = leveldb_wrapper:put_op(DB, key, [{dc3, 2}], #log_record{version = 4}),
        ok = leveldb_wrapper:put_op(DB, key, [{dc2, 2}], #log_record{version = 3}),
        ok = leveldb_wrapper:put_op(DB, key, [{dc1, 3}, {dc2, 5}], #log_record{version = 2}),
        ok = leveldb_wrapper:put_op(DB, key, [{dc1, 3}], #log_record{version = 1}),

        OPS = leveldb_wrapper:get_ops(DB, key, [{dc1, 3}], [{dc1, 3}, {dc2, 5}]),

        ?assertEqual([2, 3], filter_records_into_sorted_numbers(OPS))
                end).

put_n_snapshots(_DB, _Key, 0) ->
    ok;
put_n_snapshots(DB, Key, N) ->
    VC = vectorclock:from_list([{local, N}, {remote, N}]),
    leveldb_wrapper:put_snapshot(DB, Key, #materialized_snapshot{snapshot_time = VC, value = N}),
    put_n_snapshots(DB, Key, N - 1).

put_n_operations(_DB, _Key, 0) ->
    ok;
put_n_operations(DB, Key, N) ->
    %% For testing purposes, we use only the version in the record to identify
    %% the different ops, since it's easier than reproducing the whole record
    leveldb_wrapper:put_op(DB, Key, [{local, N}, {remote, N}],
        #log_record{version = N}),
    put_n_operations(DB, Key, N - 1).

filter_records_into_sorted_numbers(List) ->
    lists:sort(lists:foldr(fun(Record, Acum) -> [Record#log_record.version | Acum] end, [], List)).

vectorclock_to_sorted_list(VC) ->
    case is_list(VC) of
        true -> lists:sort(VC);
        false -> lists:sort(dict:to_list(VC))
    end.
