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
-module(prop_get_ops).

-define(PROPER_NO_TRANS, true).
-include_lib("proper/include/proper.hrl").

-include_lib("antidote_utils/include/antidote_utils.hrl").

-export([prop_get_ops/0, blub/0]).

blub() ->
    Ops = [{inc, dc2, 1}, {inc, dc3, 1}, {inc, dc2, 1}, {pull, dc2, dc3}, {inc, dc3, 1}],
    checkSpec(opsToClocks(Ops)).

prop_get_ops() ->
    ?FORALL(Ops, generateOps(),
        checkSpec(opsToClocks(Ops))
    ).

checkSpec(Clocks) ->
%%  io:format("Clocks = ~p~n", [Clocks]),
    VClocks = [vectorclock:from_list(C) || C <- Clocks],
    eleveldb_wrapper_SUITE:withFreshDb(
        fun(Db) ->
            % insert all the operations
            {_, Entries} = lists:foldl(
                fun(Clock, {I, Entries}) ->
                    Entry = {logEntry, I},
                    ok = leveldb_wrapper:put_op(Db, key, Clock, Entry),
                    {I + 1, [{Clock, Entry} | Entries]}
                end, {1, []}, VClocks),
            % generate all possible {From, To} pairs from the clocks, except the cases where From == To
            ClockPairs = [{From, To} || From <- VClocks, To <- VClocks, vectorclock:le(From, To) and not vectorclock:eq(From, To)],
            conjunction([{{dict:to_list(From), dict:to_list(To)}, checkGetOps(Db, Entries, From, To)} || {From, To} <- ClockPairs])
        end).

checkGetOps(Db, Entries, From, To) ->
    Records = leveldb_wrapper:get_ops(Db, key, From, To),
    Expected = [Rec ||
        {Clock, Rec} <- Entries,
        not vectorclock:lt(Clock, From) and vectorclock:le(Clock, To)
    ],
    ?WHENFAIL(
        begin
            io:format("~n---- Start of testcase -------~n"),
            [io:format("ok = put_op(Db, key, ~w, ~w),~n",
                [dict:to_list(C), E]) || {C, E} <- Entries],
            io:format("Records = get_ops(Db, key, ~w, ~w),~n",
                [dict:to_list(From), dict:to_list(To)]),
            io:format("?assertEqual(~w, filter_records_into_sorted_numbers((Records)),~n", [lists:sort(Expected)]),
            io:format("% returned ~w~n", [Records]),
            io:format("---- End of testcase -------~n")
        end,
        lists:sort(Records) == lists:sort(Expected)).

%%generateClocks() ->
%%    ?LET(Ops, generateOps(), opsToClocks(Ops)).

opsToClocks(Ops) ->
    {Clocks, _} = execOps(Ops, {[], orddict:from_list([{R, [vectorclock:new()]} || R <- replicas()])}),
    lists:sort([dict:to_list(C) || C <- Clocks]).

execOps([], State) -> State;
execOps([{pull, SourceR, TargetR} | RemainingOps], {OpClocks, State}) ->
    SourceClocks = orddict:fetch(SourceR, State),
    TargetClock = lists:last(orddict:fetch(TargetR, State)),
    NewSourceClocks = [C || C <- SourceClocks, not vectorclock:le(C, TargetClock)],
    MergedClock =
        case NewSourceClocks of
            [] -> TargetClock;
            [C | _] ->
                vectorclock:max([C, TargetClock])
        end,
    NewState = orddict:append(TargetR, MergedClock, State),
    execOps(RemainingOps, {OpClocks, NewState});
execOps([{inc, R, Amount} | RemainingOps], {OpClocks, State}) ->
    RClock = lists:last(orddict:fetch(R, State)),
    NewClock = vectorclock:set_clock_of_dc(R, Amount + vectorclock:get_clock_of_dc(R, RClock), RClock),
    NewState = orddict:append(R, NewClock, State),
    execOps(RemainingOps, {[NewClock | OpClocks], NewState}).

generateOps() ->
    list(oneof([
        % pulls one operation from the first replica to the second
        {pull, replica(), replica()},
        % increment vector clock
        {inc, replica(), range(1, 3)}
    ])).

replica() ->
    oneof(replicas()).

replicas() -> [dc1, dc2, dc3].
