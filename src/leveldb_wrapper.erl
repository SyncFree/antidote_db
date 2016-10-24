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
%% before CommitTime. If no snapshot is found, returns {error, not_found}
-spec get_snapshot(eleveldb:db_ref(), key(), snapshot_time()) ->
    {ok, #materialized_snapshot{}} | {error, not_found}.
get_snapshot(DB, Key, CommitTime) ->
    try
        eleveldb:fold(DB,
            fun({K, V}, AccIn) ->
                {Key1, _MAX, _HASH, SNAP, VC} = binary_to_term(K),
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

%% Saves the snapshot into the DB
-spec put_snapshot(antidote_db:antidote_db(), key(), #materialized_snapshot{}) -> ok | error.
put_snapshot(DB, Key, Snapshot) ->
    VCDict = vectorclock_to_dict(Snapshot#materialized_snapshot.snapshot_time),
    put(DB, {binary_to_atom(Key), get_max_time_in_VC(VCDict),
        erlang:phash2(VCDict), snap, vectorclock_to_list(VCDict)}, Snapshot).

%% Returns a list of operations that have commit time in the range [VCFrom, VCTo).
%% In other words, it returns all ops which have a VectorClock concurrent or larger than VCFrom,
%% and smaller or equal (for all entries) than VCTo.
%% Examples of what this method returns, can be seen in the tests.
-spec get_ops(eleveldb:db_ref(), key(), vectorclock(), vectorclock()) -> [#log_record{}].
get_ops(DB, Key, VCFrom, VCTo) ->
    %% Convert the VCs passed in to Dicts (if necessary)
    VCFromDict = vectorclock_to_dict(VCFrom),
    VCToDict = vectorclock_to_dict(VCTo),

    %% Calculate the min time in the VCs that compose the search range
    MinTimeToSearch = get_min_time_in_VCs(VCFromDict, VCToDict),

    %% Get the max time in the lower upper bound so the fold starts from keys that have that max value
    StartingTime = get_max_time_in_VC(VCToDict),
    try
        eleveldb:fold(DB,
            fun({K, V}, AccIn) ->
                {Key1, MAX, _HASH, OP, VC1} = binary_to_term(K),
                VC1Dict = vectorclock:from_list(VC1),
                case Key == Key1 of
                    true ->
                        %% Check that the MinTimeToSearch is smaller than the MAX value of VC1
                        case MinTimeToSearch =< MAX of
                            true ->
                                %% Check VC in range and the DB entry corresponds to an OP
                                case vc_in_range(VC1Dict, VCFromDict, VCToDict) and (OP == op) of
                                    true ->
                                        [binary_to_term(V) | AccIn];
                                    false ->
                                        AccIn
                                end;
                            false -> %% All entries of VC1 are smaller than the MinTime to search
                                throw({break, AccIn})
                        end;
                    false -> %% Not the same key we are looking for
                        throw({break, AccIn})
                end
            end,
            [],
            [{first_key, term_to_binary({Key, StartingTime})}])
    catch
        {break, OPS} ->
            OPS;
        _ ->
            []
    end.

%% If VCs have the same keys, returns the min time found,
%% otherwise returns 0.
get_min_time_in_VCs(VC1, VC2) ->
    case same_keys_in_vcs(VC1, VC2) of
        true ->
            min(get_min_time_in_VC(VC1), get_min_time_in_VC(VC2));
        false ->
            0
    end.

%% Checks that the 2 VCs passed in, have the same keys
same_keys_in_vcs(VC1, VC2) ->
    dict:fetch_keys(VC1) == dict:fetch_keys(VC2).


get_min_time_in_VC(VC) ->
    dict:fold(fun find_min_value/3, undefined, VC).

get_max_time_in_VC(VC) ->
    dict:fold(fun find_max_value/3, undefined, VC).

find_min_value(_Key, Value, Acc) ->
    case Acc of
        undefined ->
            Value;
        _ ->
            min(Value, Acc)
    end.

find_max_value(_Key, Value, Acc) ->
    case Acc of
        undefined ->
            Value;
        _ ->
            max(Value, Acc)
    end.

%% Returns true if the VC is in the required range
vc_in_range(VC, VCFrom, VCTo) ->
    not vectorclock:lt(VC, VCFrom) and vectorclock:le(VC, VCTo).

%% Saves the operation into the DB
-spec put_op(eleveldb:db_ref(), key(), vectorclock(), #log_record{}) -> ok | error.
put_op(DB, Key, VC, Record) ->
    VCDict = vectorclock_to_dict(VC),
    put(DB, {binary_to_atom(Key), get_max_time_in_VC(VCDict),
        erlang:phash2(VCDict), op, vectorclock_to_list(VC)}, Record).

vectorclock_to_dict(VC) ->
    case is_list(VC) of
        true -> vectorclock:from_list(VC);
        false -> VC
    end.

vectorclock_to_list(VC) ->
    case is_list(VC) of
        true -> VC;
        false -> dict:to_list(VC)
    end.

%% Workaround for basho bench
%% TODO find a better solution to this
binary_to_atom(Key) ->
    case is_binary(Key) of
        true -> list_to_atom(integer_to_list(binary_to_integer(Key)));
        false -> Key
    end.

%% @doc puts the Value associated to Key in eleveldb
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
    {ok, DB} = eleveldb:open("test_db", [{create_if_missing, true}, {antidote, true}]),
    try
        F(DB)
    after
        eleveldb:close(DB),
        eleveldb:destroy("test_db", [])
    end.

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
        ?assertEqual([{local, 2}, {remote, 3}], vectorclock_to_sorted_list(Snapshot#materialized_snapshot.snapshot_time)),
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

%% This test ensures that the break condition doesn't stop in the min value of DCS
%% if the VCs in the search range, have different keyset.
concurrent_op_lower_than_search_range_test() ->
    withFreshDb(fun(DB) ->
        ok = put_op(DB, key, [{dc3, 2}], #log_record{version = 4}),
        ok = put_op(DB, key, [{dc2, 2}], #log_record{version = 3}),
        ok = put_op(DB, key, [{dc1, 3}, {dc2, 5}], #log_record{version = 2}),
        ok = put_op(DB, key, [{dc1, 3}], #log_record{version = 1}),

        OPS = get_ops(DB, key, [{dc1, 3}], [{dc1, 3}, {dc2, 5}]),

        ?assertEqual([1, 2, 3], filter_records_into_sorted_numbers(OPS))
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

filter_records_into_sorted_numbers(List) ->
    lists:sort(lists:foldr(fun(Record, Acum) -> [Record#log_record.version | Acum] end, [], List)).

vectorclock_to_sorted_list(VC) ->
    case is_list(VC) of
        true -> lists:sort(VC);
        false -> lists:sort(dict:to_list(VC))
    end.

-endif.
