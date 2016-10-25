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
-export([wrong_types_passed_in/1]).

all() -> [wrong_types_passed_in].

wrong_types_passed_in(_Config) ->
    ?assertEqual({error, type_not_supported}, antidote_db:new("TEST", type)),
    ?assertEqual({error, type_not_supported}, antidote_db:close_and_destroy({type, db}, name)),
    ?assertEqual({error, type_not_supported}, antidote_db:close({type, db})),
    ?assertEqual({error, type_not_supported}, antidote_db:get_snapshot({type, db}, key, [])),
    ?assertEqual({error, type_not_supported}, antidote_db:put_snapshot({type, db}, key, [])),
    ?assertEqual({error, type_not_supported}, antidote_db:get_ops({type, db}, key, [], [])),
    ?assertEqual({error, type_not_supported}, antidote_db:put_op({type, db}, key, [], [])).