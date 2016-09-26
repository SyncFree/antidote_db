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
    get_ops_applicable_to_snapshot/3,
    get_snapshot/3,
    put_snapshot/3,
    get_ops/4,
    put_op/4]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


%% Given a key and a VC, this method returns the most suitable snapshot committed before the VC
%% with a list of operations in the range [SnapshotCommitTime, VC) to be applied to the mentioned
%% snapshot to generate a new one.
-spec get_ops_applicable_to_snapshot(eleveldb:db_ref(), key(), vectorclock()) ->
    {ok, #materialized_snapshot{} | not_found, [#log_record{}]}.
get_ops_applicable_to_snapshot(DB, Key, VectorClock) ->
    try
        Res = eleveldb:fold(DB,
            fun({K, V}, AccIn) ->
                {Key1, VC1, OP} = binary_to_term(K),
                VC1Dict = vectorclock:from_list(VC1),
                case Key == Key1 of %% check same key
                    true ->
                        %% if its greater, continue
                        case vectorclock:strict_ge(VC1Dict, VectorClock) of
                            true ->
                                AccIn;
                            false ->
                                case (OP == op) of
                                    true ->
                                        [binary_to_term(V) | AccIn];
                                    false ->
                                        throw({break, binary_to_term(V), AccIn})
                                end
                        end;
                    false ->
                        throw({break, not_found, AccIn})
                end
            end,
            [],
            [{first_key, term_to_binary({Key})}]),
        %% If the fold returned without throwing a break (it iterated all
        %% keys and ended up normally) reverse the resulting list
        {ok, not_found, lists:reverse(Res)}
    catch
        {break, Snapshot, OPS} ->
            {ok, Snapshot, lists:reverse(OPS)};
        _ ->
            {error, not_found}
    end.

%% Gets the most suitable snapshot for Key that has been committed
%% before CommitTime. If its nothing is found, returns {error, not_found}
-spec get_snapshot(eleveldb:db_ref(), key(), snapshot_time()) ->
    {ok, #materialized_snapshot{}} | {error, not_found}.
get_snapshot(DB, Key, CommitTime) ->
    try
        eleveldb:fold(DB,
            fun({K, V}, AccIn) ->
                {Key1, VC, SNAP} = binary_to_term(K),
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
    put(AntidoteDB, {binary_to_atom(Key), SnapshotTimeList, snap}, Snapshot).

%% Returns a list of operations that have commit time in the range [VCFrom, VCTo).
%% In other words, it returns all ops which have a VectorClock concurrent or larger than VCFrom,
%% and smaller or equal (for all entries) than VCTo.
%% An example on what this method returns can be seen in the test get_operations_non_empty_test.
-spec get_ops(eleveldb:db_ref(), key(), vectorclock(), vectorclock()) -> [#log_record{}].
get_ops(DB, Key, VCFrom, VCTo) ->
    VCFromDict = vectorclock_to_dict(VCFrom),
    VCToDict = vectorclock_to_dict(VCTo),
    try
        Res = eleveldb:fold(DB,
            fun({K, V}, AccIn) ->
                {Key1, VC1, OP} = binary_to_term(K),
                VC1Dict = vectorclock:from_list(VC1),
                case Key == Key1 of %% check same key
                    true ->
                        %% if its greater, continue
                        case vectorclock:strict_ge(VC1Dict, VCToDict) of
                            true ->
                                AccIn;
                            false ->
                                %% check its an op and its commit time is in the required range
                                case vectorclock:lt(VC1Dict, VCFromDict) of
                                    true ->
                                        throw({break, AccIn});
                                    false ->
                                        case (OP == op) of
                                            true ->
                                                [binary_to_term(V) | AccIn];
                                            false ->
                                                AccIn
                                        end
                                end
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

%% Saves the operation into AntidoteDB
-spec put_op(eleveldb:db_ref(), key(), vectorclock(), #log_record{}) -> ok | error.
put_op(DB, Key, VC, Record) ->
    VCList = vectorclock_to_sorted_list(VC),
    put(DB, {binary_to_atom(Key), VCList, op}, Record).

vectorclock_to_dict(VC) ->
    case is_list(VC) of
        true -> vectorclock:from_list(VC);
        false -> VC
    end.

%% Sort the resulting list, for easier comparison and parsing
vectorclock_to_sorted_list(VC) ->
    case is_list(VC) of
        true -> lists:sort(VC);
        false -> lists:sort(dict:to_list(VC))
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
    ?assertEqual([{a, 1}, {b, 2}, {c, 3}, {d, 4}, {e, 5}], Sorted).

get_ops_applicable_to_snapshot_empty_result_test() ->
    withFreshDb(fun(DB) ->
        Key = key,

        %% There are no ops nor snapshots in the DB
        NotFound = get_ops_applicable_to_snapshot(DB, Key, vectorclock:from_list([{local, 3}, {remote, 2}])),
        ?assertEqual({ok, not_found, []}, NotFound),

        %% Add a new Snapshot, but it's not in the range searched, so the result is still empty
        VC = vectorclock:from_list([{local, 4}, {remote, 4}]),
        put_snapshot(DB, Key, #materialized_snapshot{snapshot_time = VC, value = 1}),

        S1 = get_ops_applicable_to_snapshot(DB, Key, vectorclock:from_list([{local, 3}, {remote, 3}])),
        ?assertEqual({ok, not_found, []}, S1)
                end).

get_ops_applicable_to_snapshot_non_empty_result_test() ->
    withFreshDb(fun(DB) ->
        Key = key,
        Key1 = key1,

        %% Save two snapshots
        VC = vectorclock:from_list([{local, 4}, {remote, 4}]),
        put_snapshot(DB, Key, #materialized_snapshot{snapshot_time = VC, value = 1}),
        VC1 = vectorclock:from_list([{local, 2}, {remote, 2}]),
        put_snapshot(DB, Key, #materialized_snapshot{snapshot_time = VC1, value = 2}),

        %% There is one [4, 4] snapshot matches best so it's returned with no ops since there aren't any
        {ok, S1, Ops1} = get_ops_applicable_to_snapshot(DB, Key, vectorclock:from_list([{local, 8}, {remote, 9}])),
        ?assertEqual(1, S1#materialized_snapshot.value),
        ?assertEqual([], Ops1),

        %% Add some ops, and try again with the same VC. Now ops to be applied are returned
        put_n_operations(DB, Key, 10),
        put_n_operations(DB, Key1, 5),

        {ok, S2, Ops2} = get_ops_applicable_to_snapshot(DB, Key, vectorclock:from_list([{local, 8}, {remote, 9}])),
        ?assertEqual(1, S2#materialized_snapshot.value),
        ?assertEqual([8, 7, 6, 5, 4], filter_records_into_numbers(Ops2))
                end).

get_snapshot_not_found_test() ->
    withFreshDb(fun(DB) ->
        Key = key,
        Key1 = key1,
        Key2 = key2,

        %% No snapshot in the DB
        NotFound = get_snapshot(DB, Key, vectorclock:from_list([{local, 0}, {remote, 0}])),
        ?assertEqual({error, not_found}, NotFound),

        %% Put 10 snapshots for Key and check there is no snapshot with time 0 in both DCs
        put_n_snapshots(DB, Key, 10),
        NotFound1 = get_snapshot(DB, Key, vectorclock:from_list([{local, 0}, {remote, 0}])),
        ?assertEqual({error, not_found}, NotFound1),

        %% Look for a snapshot for Key1
        S1 = get_snapshot(DB, Key1, vectorclock:from_list([{local, 5}, {remote, 4}])),
        ?assertEqual({error, not_found}, S1),

        %% Put snapshots for Key2 and look for a snapshot for Key1
        put_n_snapshots(DB, Key2, 10),
        S2 = get_snapshot(DB, Key1, vectorclock:from_list([{local, 5}, {remote, 4}])),
        ?assertEqual({error, not_found}, S2)
                end).

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

        %% Concurrent operations int the lower bound are present in the result
        O1 = get_ops(DB, Key1, [{local, 2}, {remote, 2}], [{local, 8}, {remote, 9}]),
        ?assertEqual([8, 7, 6, 5, 4, 3, 2], filter_records_into_numbers(O1)),

        O2 = get_ops(DB, Key1, [{local, 4}, {remote, 5}], [{local, 7}, {remote, 7}]),
        ?assertEqual([7, 6, 5, 4], filter_records_into_numbers(O2))
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
        ?assertEqual([{local, 2}, {remote, 3}], vectorclock_to_sorted_list(Snapshot#materialized_snapshot.snapshot_time)),
        ?assertEqual(5, Snapshot#materialized_snapshot.value),

        O1 = get_ops(DB, Key1, Snapshot#materialized_snapshot.snapshot_time, VCTo),
        ?assertEqual([7, 6, 5, 4, 3, 2], filter_records_into_numbers(O1))
                end).

%% This test is used to check that compare function for VCs is working OK
%% with VCs containing != lengths and values
length_of_vc_test() ->
    withFreshDb(fun(DB) ->
        %% Same key, and same value for the local DC
        %% OP2 should be newer than op1 since it contains 1 more DC in its VC
        Key = key,
        put_op(DB, Key, [{local, 2}], #log_record{version = 1}),
        put_op(DB, Key, [{local, 2}, {remote, 3}], #log_record{version = 2}),
        O1 = filter_records_into_numbers(get_ops(DB, Key, [{local, 1}, {remote, 1}], [{local, 7}, {remote, 8}])),
        ?assertEqual([2, 1], O1),

        %% Insert OP3, with no remote DC value and check it´s newer than 1 and 2
        put_op(DB, Key, [{local, 3}], #log_record{version = 3}),
        O2 = get_ops(DB, Key, [{local, 1}, {remote, 1}], [{local, 7}, {remote, 8}]),
        ?assertEqual([3, 2, 1], filter_records_into_numbers(O2)),

        %% OP3 is still returned if the local value we look for is lower
        %% This is the expected outcome for vectorclock gt and lt methods
        O3 = get_ops(DB, Key, [{local, 1}, {remote, 1}], [{local, 2}, {remote, 8}]),
        ?assertEqual([3, 2, 1], filter_records_into_numbers(O3)),

        %% Insert remote operation not containing local clock and check is the oldest one
        put_op(DB, Key, [{remote, 1}], #log_record{version = 4}),
        O4 = get_ops(DB, Key, [{local, 1}, {remote, 1}], [{local, 7}, {remote, 8}]),
        ?assertEqual([3, 2, 1, 4], filter_records_into_numbers(O4))
                end).

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

filter_records_into_numbers(List) ->
    lists:foldr(fun(Record, Acum) -> [Record#log_record.version | Acum] end, [], List).

-endif.
