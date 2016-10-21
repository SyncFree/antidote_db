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
-module(leveldb_wrapper).

-include_lib("antidote_utils/include/antidote_utils.hrl").

-export([
    get_snapshot/3,
    put_snapshot/3,
    get_ops/4,
    put_op/4]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Gets the most suitable snapshot for Key that has been committed
%% before CommitTime. If its nothing is found, returns {error, not_found}
-spec get_snapshot(eleveldb:db_ref(), key(), snapshot_time()) ->
    {ok, #materialized_snapshot{}} | {error, not_found}.
get_snapshot(DB, Key, CommitTime) ->
    try
        eleveldb:fold(DB,
            fun({K, V}, AccIn) ->
                {Key1, VC, _HASH, SNAP} = binary_to_term(K),
                case (Key1 == Key) of %% check same key
                    true ->
                        %% check its a snapshot and its time is less than the one required
                        case (SNAP == snap) and
                            vectorclock:le(vectorclock:from_list(VC), CommitTime) of
                            true ->
                                Snapshot = binary_to_term(V),
                                throw({break, Snapshot});
                            _ ->
                                AccIn
                        end;
                    false ->
                        throw({break})
                end
            end,
            [],
            [{first_key, term_to_binary({Key})}]),
        {error, not_found}
    catch
        {break, SNAP} ->
            {ok, SNAP};
        _ ->
            {error, not_found}
    end.

%% Saves the snapshot into AntidoteDB
-spec put_snapshot(antidote_db:antidote_db(), key(), #materialized_snapshot{}) -> ok | error.
put_snapshot(AntidoteDB, Key, Snapshot) ->
    SnapshotTimeList = vectorclock_to_sorted_list(Snapshot#materialized_snapshot.snapshot_time),
    put(AntidoteDB, {binary_to_atom(Key), SnapshotTimeList, erlang:phash2(SnapshotTimeList), snap}, Snapshot).

%% Returns a list of operations that have commit time in the range [VCFrom, VCTo).
%% In other words, it returns all ops which have a VectorClock concurrent or larger than VCFrom,
%% and smaller or equal (for all entries) than VCTo.
%% An example on what this method returns can be seen in the test get_operations_non_empty_test.
-spec get_ops(eleveldb:db_ref(), key(), vectorclock(), vectorclock()) -> [#log_record{}].
get_ops(DB, Key, VCFrom, VCTo) ->
    VCFromDict = vectorclock_to_dict(VCFrom),
    VCToDict = vectorclock_to_dict(VCTo),
    MinTimeToSearch = get_min_time_in_VCs(VCFromDict, VCToDict),
    try
        Res = eleveldb:fold(DB,
            fun({K, V}, AccIn) ->
                {Key1, VC1, _HASH, OP} = binary_to_term(K),
                VC1Dict = vectorclock:from_list(VC1),
                io:format("~p : ~p ~n", [binary_to_term(K), binary_to_term(V)]),
                case Key == Key1 of %% check same key
                    true ->
                        %% if its greater, continue
                        io:format("le or conc ~p    ~p ~n", [vectorclock:le(VC1Dict, VCToDict), concurrent_VCs(VC1Dict, VCToDict)]),
                        case vectorclock:le(VC1Dict, VCToDict) or concurrent_VCs(VC1Dict, VCToDict) of
                            true ->
                                %% check its an op and its commit time is in the required range
                                io:format("MIN ~p : ~p   RES: ~p ~n", [MinTimeToSearch, element(2, lists:nth(1, VC1)), MinTimeToSearch > element(2, lists:nth(1, VC1))]),
                                case MinTimeToSearch > element(2, lists:nth(1, VC1)) of
                                    true ->
                                        throw({break, AccIn});
                                    false ->
                                        io:format("Greater or conc ~p ~n", [greater_or_concurrent_VC(VCFromDict, VC1Dict)]),
                                        case greater_or_concurrent_VC(VC1Dict, VCFromDict) and (OP == op) of
                                            true ->
                                                [binary_to_term(V) | AccIn];
                                            false ->
                                                AccIn
                                        end
                                end;
                            false ->
                                AccIn
                        end;
                    false ->
                        throw({break, AccIn})
                end
            end,
            [],
            [{first_key, term_to_binary({Key})}]),
        %% If the fold returned without throwing a break (it iterated all
        %% keys and ended up normally) reverse the resulting list
        lists:reverse(Res)
    catch
        {break, OPS} ->
            lists:reverse(OPS);
        _ ->
            []
    end.

get_min_time_in_VCs(VC1, VC2) ->
    VC1List = vectorclock_to_sorted_list(VC1),
    VC2List = vectorclock_to_sorted_list(VC2),
    {_, MinTimeToSearch1} = lists:nth(length(VC1List), VC1List),
    {_, MinTimeToSearch2} = lists:nth(length(VC2List), VC2List),
    min(MinTimeToSearch1, MinTimeToSearch2).

%% Returns true if VC1 is greater or concurrent with VC2
greater_or_concurrent_VC(VC1, VC2) ->
    vectorclock:ge(VC1, VC2) or concurrent_VCs(VC1, VC2).

%% Returns true if VC1 is concurrent with VC2
concurrent_VCs(VC1, VC2) ->
    not (vectorclock:ge(VC1, VC2)) and not (vectorclock:le(VC1, VC2)).

%% Saves the operation into AntidoteDB
-spec put_op(eleveldb:db_ref(), key(), vectorclock(), #log_record{}) -> ok | error.
put_op(DB, Key, VC, Record) ->
    VCList = vectorclock_to_sorted_list(VC),
    put(DB, {binary_to_atom(Key), VCList, erlang:phash2(VCList), op}, Record).

vectorclock_to_dict(VC) ->
    case is_list(VC) of
        true -> vectorclock:from_list(VC);
        false -> VC
    end.

%% Sort the resulting list, for easier comparison and parsing
vectorclock_to_sorted_list(VC) ->
    case is_list(VC) of
        true -> lists:sort(fun({_, ValA}, {_, ValB}) -> ValA > ValB end, VC);
        false -> lists:sort(fun({_, ValA}, {_, ValB}) -> ValA > ValB end, dict:to_list(VC))
    end.

%% Workaround for basho bench
%% TODO find a better solution to this
binary_to_atom(Key) ->
    case is_binary(Key) of
        true -> list_to_atom(integer_to_list(binary_to_integer(Key)));
        false -> Key
    end.

%% @doc puts the Value associated to Key in eleveldb AntidoteDB
-spec put(eleveldb:db_ref(), any(), any()) -> ok | {error, any()}.
put(DB, Key, Value) ->
    AKey = case is_binary(Key) of
               true -> Key;
               false -> term_to_binary(Key)
           end,
    ATerm = case is_binary(Value) of
                true -> Value;
                false -> term_to_binary(Value)
            end,
    eleveldb:put(DB, AKey, ATerm, []).

-ifdef(TEST).

withFreshDb(F) ->
    %% Destroy the test DB to prevent having dirty DBs if a test fails
    eleveldb:destroy("test_db", []),
    {ok, AntidoteDB} = antidote_db:new("test_db", leveldb),
    {leveldb, Db} = AntidoteDB,
    try
        F(Db)
    after
        antidote_db:close_and_destroy(AntidoteDB, "test_db")
    end.

%% This test ensures vectorclock_to_list method
%% sorts VCs the correct way
vectorclock_to_sorted_list_test() ->
    Sorted = vectorclock_to_sorted_list([{e, 5}, {c, 3}, {a, 1}, {b, 2}, {d, 4}]),
    ?assertEqual([{e, 5}, {d, 4}, {c, 3}, {b, 2}, {a, 1}], Sorted).

get_snapshot_matching_vc_test() ->
    withFreshDb(fun(DB) ->

        Key = key,
        put_n_snapshots(DB, Key, 10),

        %% Get some of the snapshots inserted (matching VC)
        {ok, S1} = get_snapshot(DB, Key, vectorclock:from_list([{local, 1}, {remote, 1}])),
        {ok, S2} = get_snapshot(DB, Key, vectorclock:from_list([{local, 4}, {remote, 4}])),
        {ok, S3} = get_snapshot(DB, Key, vectorclock:from_list([{local, 8}, {remote, 8}])),

        ?assertEqual([{local, 1}, {remote, 1}], vectorclock_to_sorted_list(S1#materialized_snapshot.snapshot_time)),
        ?assertEqual(1, S1#materialized_snapshot.value),

        ?assertEqual([{local, 4}, {remote, 4}], vectorclock_to_sorted_list(S2#materialized_snapshot.snapshot_time)),
        ?assertEqual(4, S2#materialized_snapshot.value),

        ?assertEqual([{local, 8}, {remote, 8}], vectorclock_to_sorted_list(S3#materialized_snapshot.snapshot_time)),
        ?assertEqual(8, S3#materialized_snapshot.value)
                end).

get_snapshot_not_matching_vc_test() ->
    withFreshDb(fun(DB) ->
        Key = key,

        %% Add 3 snapshots
        VC = vectorclock:from_list([{local, 4}, {remote, 4}]),
        put_snapshot(DB, Key, #materialized_snapshot{snapshot_time = VC, value = 4}),

        VC1 = vectorclock:from_list([{local, 2}, {remote, 3}]),
        put_snapshot(DB, Key, #materialized_snapshot{snapshot_time = VC1, value = 2}),

        VC2 = vectorclock:from_list([{local, 8}, {remote, 7}]),
        put_snapshot(DB, Key, #materialized_snapshot{snapshot_time = VC2, value = 8}),

        %% Request the snapshots using a VC different than the one used to insert them
        S4 = get_snapshot(DB, Key, vectorclock:from_list([{local, 1}, {remote, 0}])),
        {ok, S5} = get_snapshot(DB, Key, vectorclock:from_list([{local, 6}, {remote, 5}])),
        {ok, S6} = get_snapshot(DB, Key, vectorclock:from_list([{local, 8}, {remote, 9}])),

        ?assertEqual({error, not_found}, S4),

        ?assertEqual([{local, 4}, {remote, 4}], vectorclock_to_sorted_list(S5#materialized_snapshot.snapshot_time)),
        ?assertEqual(4, S5#materialized_snapshot.value),

        ?assertEqual([{local, 8}, {remote, 7}], vectorclock_to_sorted_list(S6#materialized_snapshot.snapshot_time)),
        ?assertEqual(8, S6#materialized_snapshot.value)
                end).

get_operations_empty_result_test() ->
    withFreshDb(fun(DB) ->
        Key = key,
        Key1 = key1,

        %% Nothing in the DB returns an empty list
        O1 = get_ops(DB, Key, [{local, 2}, {remote, 2}], [{local, 8}, {remote, 9}]),
        ?assertEqual([], O1),

        %% Insert some operations
        put_n_operations(DB, Key, 10),

        %% Requesting for ops in a range with noting, returns an empty list
        O2 = get_ops(DB, Key, [{local, 123}, {remote, 100}], [{local, 200}, {remote, 124}]),
        ?assertEqual([], O2),

        %% Getting a key not present, returns an empty list
        O3 = get_ops(DB, Key1, [{local, 2}, {remote, 2}], [{local, 4}, {remote, 5}]),
        ?assertEqual([], O3),

        %% Searching for the same range returns an empty list
        O4 = get_ops(DB, Key1, [{local, 2}, {remote, 2}], [{local, 2}, {remote, 2}]),
        ?assertEqual([], O4)
                end).

get_operations_non_empty_test() ->
    withFreshDb(fun(DB) ->
        %% Fill the DB with values
        Key = key,
        Key1 = key1,
        Key2 = key2,

        put_n_operations(DB, Key, 100),
        put_n_operations(DB, Key1, 10),
        put_n_operations(DB, Key2, 25),

        O1 = get_ops(DB, Key1, [{local, 2}, {remote, 2}], [{local, 8}, {remote, 9}]),
        ?assertEqual([2, 3, 4, 5, 6, 7, 8], filter_records_into_sorted_numbers(O1)),

        O2 = get_ops(DB, Key1, [{local, 4}, {remote, 5}], [{local, 7}, {remote, 7}]),
        ?assertEqual([5, 6, 7], filter_records_into_sorted_numbers(O2))
                end).

operations_and_snapshots_mixed_test() ->
    withFreshDb(fun(DB) ->
        Key = key,
        Key1 = key1,
        Key2 = key2,

        VCTo = [{local, 7}, {remote, 8}],
        put_n_operations(DB, Key, 10),
        put_n_operations(DB, Key1, 20),
        put_snapshot(DB, Key1, #materialized_snapshot{snapshot_time = [{local, 2}, {remote, 3}], value = 5}),
        put_n_operations(DB, Key2, 8),

        %% We want all ops for Key1 that are between the snapshot and
        %% [{local, 7}, {remote, 8}]. First get the snapshot, then OPS.
        {ok, Snapshot} = get_snapshot(DB, Key1, vectorclock:from_list(VCTo)),
        ?assertEqual([{remote, 3}, {local, 2}], vectorclock_to_sorted_list(Snapshot#materialized_snapshot.snapshot_time)),
        ?assertEqual(5, Snapshot#materialized_snapshot.value),

        O1 = get_ops(DB, Key1, Snapshot#materialized_snapshot.snapshot_time, VCTo),
        ?assertEqual([3, 4, 5, 6, 7], filter_records_into_sorted_numbers(O1))
                end).

%% This test inserts 5 ops, 4 of them are concurrent, and checks that only the first and two of the concurrent are
%% returned, since they are the only ones that match the requested ranged passed in
concurrent_ops_test() ->
    withFreshDb(fun(DB) ->
        ok = put_op(DB, d, [{dc1, 10}, {dc2, 14}, {dc3, 3}], #log_record{version = 1}),
        ok = put_op(DB, d, [{dc1, 9}, {dc2, 12}, {dc3, 1}], #log_record{version = 2}),
        ok = put_op(DB, d, [{dc1, 7}, {dc2, 2}, {dc3, 12}], #log_record{version = 3}),
        ok = put_op(DB, d, [{dc1, 5}, {dc2, 2}, {dc3, 10}], #log_record{version = 4}),
        ok = put_op(DB, d, [{dc1, 1}, {dc2, 1}, {dc3, 1}], #log_record{version = 5}),

        OPS = get_ops(DB, d, [{dc1, 10}, {dc2, 17}, {dc3, 2}], [{dc1, 12}, {dc2, 20}, {dc3, 18}]),

        ?assertEqual([1, 3, 4], filter_records_into_sorted_numbers(OPS))
                end).

smallest_op_returned_test() ->
    withFreshDb(fun(DB) ->
        ok = put_op(DB, key, [{dc3, 1}], #log_record{version = 4}),
        ok = put_op(DB, key, [{dc2, 4}, {dc3, 1}], #log_record{version = 3}),
        ok = put_op(DB, key, [{dc2, 2}], #log_record{version = 2}),
        ok = put_op(DB, key, [{dc2, 1}], #log_record{version = 1}),

        OPS = get_ops(DB, key, [{dc2, 3}], [{dc2, 3}, {dc3, 1}]),

        ?assertEqual([4], filter_records_into_sorted_numbers(OPS))
                end).

%% Checks that the dcs names don't matter for this way of ordering VCs
%% The comparator yields the same result, but the HASH of the VC makes the difference
insert_same_ops_for_sorting_test() ->
    withFreshDb(fun(DB) ->
        ok = put_op(DB, key, [{dc1, 3}], #log_record{version = 1}),
        ok = put_op(DB, key, [{dc2, 3}], #log_record{version = 2}),
        ok = put_op(DB, key, [{dc3, 3}], #log_record{version = 3}),

        print_DB(DB),

        OPS = get_ops(DB, key, [{dc0, 2}], [{dc6, 9}]),

        ?assertEqual([1, 2, 3], filter_records_into_sorted_numbers(OPS))
                end).

pepe_test() ->
    withFreshDb(fun(DB) ->
        ok = put_op(DB, key, [{dc2, 3}], #log_record{version = 1}),
        ok = put_op(DB, key, [{dc1, 5}, {dc2, 3}], #log_record{version = 2}),
        ok = put_op(DB, key, [{dc1, 3}], #log_record{version = 3}),
        ok = put_op(DB, key, [{dc1, 2}], #log_record{version = 4}),

        Records = get_ops(DB, key, [{dc1, 3}], [{dc1, 5}, {dc2, 3}]),

        ?assertEqual([1, 2, 3], filter_records_into_sorted_numbers(Records))
                end).

pepe1_test() ->
    withFreshDb(fun(DB) ->
        ok = put_op(DB, key, [{dc3, 1}], #log_record{version = 1}),
        ok = put_op(DB, key, [{dc1, 7}, {dc3, 1}], #log_record{version = 2}),
        ok = put_op(DB, key, [{dc1, 5}], #log_record{version = 3}),
        ok = put_op(DB, key, [{dc1, 2}], #log_record{version = 4}),

        Records = get_ops(DB, key, [{dc1, 5}], [{dc1, 7}, {dc3, 1}]),

        ?assertEqual([1, 2, 3], filter_records_into_sorted_numbers(Records))
                end).
pepe2_test() ->
    withFreshDb(fun(DB) ->

        ok = put_op(DB, key, [{dc3, 3}], #log_record{version = 1}),
        ok = put_op(DB, key, [{dc3, 1}], #log_record{version = 2}),
        Records = get_ops(DB, key, [{dc3, 3}], [{dc3, 3}]),

%%         VC1 = dict:from_list([{dc3, 2}]),
%%        VC2 = dict:from_list([{dc1, 3}]),
%%        io:format("COMP ~p ~p ~n", [vectorclock:ge(VC1, VC2), vectorclock:le(VC1, VC2)]),

        ?assertEqual([1], filter_records_into_sorted_numbers(Records))
                end).

%%        VC1 = dict:from_list([{dc1, 1}, {dc2, 2}]),
%%        VC2 = dict:from_list([{dc1, 2}, {dc2, 1}]),
%%        io:format("COMP ~p ~p ~n", [vectorclock:ge(VC1, VC2), vectorclock:le(VC1, VC2)]),
print_DB(Ref) ->
    io:format("----------------------------~n"),
    eleveldb:fold(
        Ref,
        fun({K, V}, AccIn) ->
            io:format("~p : ~p ~n", [binary_to_term(K), binary_to_term(V)]),
            AccIn
        end,
        [],
        []),
    io:format("----------------------------~n").

put_n_snapshots(_DB, _Key, 0) ->
    ok;
put_n_snapshots(DB, Key, N) ->
    VC = vectorclock:from_list([{local, N}, {remote, N}]),
    put_snapshot(DB, Key, #materialized_snapshot{snapshot_time = VC, value = N}),
    put_n_snapshots(DB, Key, N - 1).

put_n_operations(_DB, _Key, 0) ->
    ok;
put_n_operations(DB, Key, N) ->
    %% For testing purposes, we use only the version in the record to identify
    %% the different ops, since it's easier than reproducing the whole record
    put_op(DB, Key, [{local, N}, {remote, N}],
        #log_record{version = N}),
    put_n_operations(DB, Key, N - 1).

filter_records_into_sorted_numbers(List) ->
    lists:sort(lists:foldr(fun(Record, Acum) -> [Record#log_record.version | Acum] end, [], List)).

-endif.
