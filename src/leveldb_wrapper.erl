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
    StartingTime = get_max_time_in_VC(CommitTime),
    try
        eleveldb:fold(DB,
            fun({K, V}, AccIn) ->
                {Key1, _MAX, _HASH, SNAP, VC} = binary_to_term(K),
                case (Key1 == Key) of %% check same key
                    true ->
                        %% check it's a snapshot and has time less than the one required
                        case (SNAP == snap) and
                            not vectorclock:gt(vectorclock:from_list(VC), CommitTime) of
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
            [{first_key, term_to_binary({Key, StartingTime})}]),
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
-spec get_ops(eleveldb:db_ref(), key(), vectorclock(), vectorclock()) -> [any()].
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
    case dict:fold(fun find_min_value/3, undefined, VC) of
        undefined ->
            0;
        Val ->
            Val
    end.

get_max_time_in_VC(VC) ->
    dict:fold(fun find_max_value/3, 0, VC).

find_min_value(_Key, Value, Acc) ->
    min(Value, Acc).

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
-spec put_op(eleveldb:db_ref(), key(), vectorclock(), any()) -> ok | error.
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

empty_vc_max_min_test() ->
    VC = vectorclock:new(),
    ?assertEqual(0, get_max_time_in_VC(VC)),
    ?assertEqual(0, get_min_time_in_VC(VC)).

non_empty_vc_max_min_test() ->
    VC = vectorclock:from_list([{dc1, 10}, {dc2, 14}, {dc3, 3}]),
    ?assertEqual(14, get_max_time_in_VC(VC)),
    ?assertEqual(3, get_min_time_in_VC(VC)).

-endif.
