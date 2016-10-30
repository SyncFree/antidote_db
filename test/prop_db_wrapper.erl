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
-module(prop_db_wrapper).

-behaviour(proper_statem).

-define(PROPER_NO_TRANS, true).
-include_lib("proper/include/proper.hrl").
-include_lib("antidote_utils/include/antidote_utils.hrl").

-export([command/1, initial_state/0, next_state/3, precondition/2, postcondition/3, put_op/3, get_ops/3]).
-export([prop_ops_put_get/0]).

-record(state, {
    operations = [] :: [{key(), vectorclock(), any()}]
}).

withFreshDb(F) ->
    {ok, Db} = antidote_db:new("test_db", leveldb),
    try
        F(Db)
    after
        antidote_db:close_and_destroy(Db, "test_db")
    end.

prop_ops_put_get() ->
    ?FORALL(Cmds, commands(?MODULE),
        begin
            withFreshDb(fun(Db) ->
                put(antidote_db_instance, Db),
                {_, _, Result} = run_commands(?MODULE, Cmds),
                % io:format("Result = ~p~n", [Result]),
                Result =:= ok
                        end)
        end).

command(_S) ->
    oneof([
        {call, ?MODULE, put_op, [randomKey(), randomVectorclock(), randomLogEntry()]},
        {call, ?MODULE, get_ops, [randomKey(), randomVectorclock(), randomVectorclock()]}
    ]).

put_op(Key, Clock, LogEntry) ->
    Db = get(antidote_db_instance),
    antidote_db:put_op(Db, Key, vectorclock:from_list(Clock), LogEntry).

get_ops(Key, FromClock, ToClock) ->
    Db = get(antidote_db_instance),
    antidote_db:get_ops(Db, Key, vectorclock:from_list(FromClock), vectorclock:from_list(ToClock)).

initial_state() ->
    #state{}.

next_state(S, _V, {call, _, put_op, [Key, Clock, Entry]}) ->
    % record bla
    S#state{operations = [{Key, Clock, Entry} | S#state.operations]};
next_state(S, _V, {call, _, get_ops, [_Key, _From, _To]}) ->
    S.

precondition(_S, {call, _, get_ops, [_Key, From, To]}) ->
    FromVc = vectorclock:from_list(From),
    ToVc = vectorclock:from_list(To),
    vectorclock:le(FromVc, ToVc);
precondition(_S, {call, _, put_op, [_Key, _Clock, _Entry]}) -> true.

postcondition(_S, {call, _, put_op, [_Key, _Clock, _Entry]}, _Res) ->
    true;
postcondition(S, {call, _, get_ops, [Key, From, To]}, Res) ->
    FromVc = vectorclock:from_list(From),
    ToVc = vectorclock:from_list(To),
    ModelOps = [Op || {K, C, Op} <- S#state.operations, K == Key
        , not vectorclock:le(vectorclock:from_list(C), FromVc)
        , vectorclock:le(vectorclock:from_list(C), ToVc)],
    % io:format("Expected: ~p, Actual: ~p~n", [ModelOps, Res]),
    lists:sort(ModelOps) == lists:sort(Res).

randomKey() ->
    elements([keyA, keyB, keyC]).

randomVectorclock() ->
    ?LET(T, vector(3, randomDot()), case T of [X1, X2, X3] ->
        makeDot(dc1, X1) ++ makeDot(dc2, X2) ++ makeDot(dc3, X3) end).
%L = non_empty(list(tuple([randomDc(), choose(0, 20)]))),
%L = [tuple([randomDc(), choose(0, 1000)]) |  list(tuple([randomDc(), choose(0, 1000)]))],
%L.

makeDot(_Dc, 0) -> [];
makeDot(Dc, Time) -> [{Dc, Time}].

randomDot() ->
    frequency([
        {1, 0},
        {9, choose(0, 20)}
    ]).

%%randomDc() ->
%%  elements([dc1, dc2, dc3]).

randomLogEntry() ->
    elements([x, y, z]).

