-module(antidote_db).

-export([
    new/1,
    get/2,
    put/3,
    close_and_destroy/2,
    close/1,
    fold/3,
    fold_keys/3,
    is_empty/1,
    delete/2,
    repair/1]).

-type antidote_db() :: eleveldb:db_ref().

-export_type([antidote_db/0]).

%% Given a name, returns a new AntidoteDB (ElevelDB)
%% OpenOptions are set to use Antidote special comparator
-spec new(atom()) -> {ok, antidote_db()} | {error, any()}.
new(Name) ->
    eleveldb:open(Name, [{create_if_missing, true}, {antidote, true}]).

%% Closes and destroys the given base
-spec close_and_destroy(antidote_db(), atom()) -> ok | {error, any()}.
close_and_destroy(AntidoteDB, Name) ->
    eleveldb:close(AntidoteDB),
    eleveldb:destroy(Name, []).

-spec close(antidote_db()) -> ok | {error, any()}.
close(AntidoteDB) ->
    eleveldb:close(AntidoteDB).

%% @doc returns the value of Key, in the antidote_db
-spec get(antidote_db(), atom() | binary()) -> term() | not_found.
get(AntidoteDB, Key) ->
    AKey = case is_binary(Key) of
               true -> Key;
               false -> atom_to_binary(Key, utf8)
           end,
    case eleveldb:get(AntidoteDB, AKey, []) of
        {ok, Res} ->
            binary_to_term(Res);
        not_found ->
            not_found
    end.

%% @doc puts the Value associated to Key in eleveldb AntidoteDB
-spec put(antidote_db(), atom() | binary(), any()) -> ok | {error, any()}.
put(AntidoteDB, Key, Value) ->
    AKey = case is_binary(Key) of
               true -> Key;
               false -> atom_to_binary(Key, utf8)
           end,
    ATerm = case is_binary(Value) of
                true -> Value;
                false -> term_to_binary(Value)
            end,
    eleveldb:put(AntidoteDB, AKey, ATerm, []).

-spec fold(antidote_db(), eleveldb:fold_fun(), any(), eleveldb:read_options()) -> any().
fold(AntidoteDB, Fun, Acc0, Opts) ->
    eleveldb:fold(AntidoteDB, Fun, Acc0, Opts).

-spec fold_keys(antidote_db(), eleveldb:fold_keys_fun(), any(), eleveldb:read_options()) -> any().
fold_keys(AntidoteDB, Fun, Acc0, Opts) ->
    eleveldb:fold_keys(AntidoteDB, Fun, Acc0, Opts).

-spec is_empty(antidote_db()) -> boolean().
is_empty(AntidoteDB) ->
    eleveldb:is_empty(AntidoteDB).

repair(Name) ->
    eleveldb:repair(Name, []).

-spec delete(antidote_db(), binary()) -> ok | {error, any()}.
delete(AntidoteDB, Key) ->
    eleveldb:delete(AntidoteDB, Key, []).
