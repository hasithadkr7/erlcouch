%%%-------------------------------------------------------------------
%%% @author hasitha
%%% @copyright (C) 2023, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Jan 2023 10:08 PM
%%%-------------------------------------------------------------------
-module(erlcouch_conn_repo).
-author("hasitha").

-behaviour(gen_server).

%% API
-export([
    child_spec/1,
    start_link/1,
    set/2,
    get/1,
    remove/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(SERVER, ?MODULE).
-define(CONN, doc_repo_conn).
-define(RECONNECTION_TIME, 5000).
-define(EXPIRATION, 0).


%%%===================================================================
%%% API
%%%===================================================================

-spec child_spec(map()) -> map().
child_spec(DbConfig) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [DbConfig]},
        restart => permanent
    }.

-spec start_link(map()) -> {ok, pid()} | ignore | {error, term()}.
start_link(DbConfig) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, DbConfig, []).

-spec set(binary(), any()) -> ok | {error, any()}.
set(Key, Value) ->
    gen_server:call(?MODULE, {set, Key, Value}).

-spec get(binary()) -> any() | {error, any()}.
get(Key) ->
    gen_server:call(?MODULE, {get, Key}).

-spec remove(binary()) -> ok | {error, any()}.
remove(Key) ->
    gen_server:call(?MODULE, {remove, Key}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


%% @hidden
init(DbConfig) ->
    self() ! connect,
    {ok, DbConfig}.

%% @hidden
handle_info(connect, State) ->
    ok = connect(State),
    {noreply, State};
handle_info(Msg, State) ->
    unhandled_msg(Msg, State).

%% @hidden
handle_call({set, Key, Value}, _From, State) ->
    ok = ensure_conn(State),
    case cberl:set(?CONN, Key, ?EXPIRATION, Value, raw_binary) of
        ok    -> {reply, ok, State};
        Error -> {reply, Error, State}
    end;
handle_call({get, Key}, _From, State) ->
    ok = ensure_conn(State),
    case cberl:get(?CONN, Key) of
        {Key, _, Val} -> {reply, Val, State};
        Error         -> {reply, Error, State}
    end;
handle_call({remove, Key}, _From, State) ->
    ok = ensure_conn(State),
    case cberl:remove(?CONN, Key) of
        ok    -> {reply, ok, State};
        Error -> {reply, Error, State}
    end;
handle_call(Msg, _From, State) ->
    unhandled_msg(Msg, State).

%% @hidden
handle_cast(Msg, State) ->
    unhandled_msg(Msg, State).

%%%===================================================================
%%% Internal functions
%%%===================================================================
%% @hidden
connect(#{bucket := Bucket, user_name := Username, password := Password, host := Host,
        number_of_connections := NumConn}=_State) ->
    case cberl:start_link(?CONN, NumConn, Host, Username, Password, Bucket, erlcouch_document_transcoder) of
        {ok, _Conn} -> ok;
        {error, {already_started, _}} -> ok
    end.

ensure_conn(State) ->
    case whereis(?CONN) of
        undefined -> connect(State);
        Pid when is_pid(Pid) -> ok
    end.

unhandled_msg(Msg, State) ->
    io:format("Unhandled message ~p", [Msg]),
    {noreply, State}.