-module(ets_lock).

-behaviour(gen_server).

%% API
-export([
        start_link/1,
        locked_keys/1,
        with/3,
        with/4,
        with/5
    ]).

%% gen_server callbacks
-export([
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3
    ]).

-record(state, {
        locked_set, % set of locked keys
        locks       % pid -> {monitor_ref, locked_keys} dict
    }).

-define(DEFAULT_MAX_SLEEP, 16).
-define(DEFAULT_TIMEOUT, 5000).
-define(LOCK_TIMEOUT, 1000).
-define(UNLOCK_TIMEOUT, 10*?LOCK_TIMEOUT).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Table) when is_atom(Table) ->
    gen_server:start_link({local, server_name(Table)}, ?MODULE, [Table], []).

locked_keys(Table) when is_atom(Table) ->
    {ok, LockedSet} = call(Table, locked_keys),
    sets:to_list(LockedSet).

with(Table, [_|_] = KeyList, Fun) ->
   with(Table, KeyList, Fun, ?DEFAULT_TIMEOUT). 

with(Table, [_|_] = KeyList, Fun, Timeout) ->
    with(Table, KeyList, Fun, Timeout, ?DEFAULT_MAX_SLEEP).

with(Table, [_|_] = KeyList, Fun, Timeout, MaxSleep) ->
    KeySet = sets:from_list(KeyList),
    with_priv(Table, KeyList, KeySet, Fun, Timeout, MaxSleep).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Table]) ->
    ets:new(Table, [public, named_table]),
    {ok, #state{ locked_set = sets:new(), locks = dict:new() }}.

handle_call({lock, KeySet}, {Pid, _Tag}, #state{ locked_set = LockedSet, locks = LocksDict } = State) ->
    {Rslt, NewState} = 
        case sets:is_disjoint(KeySet, LockedSet) of
            true -> 
                {Monitor, PidKeySet} =
                    case dict:find(Pid, LocksDict) of
                        {ok, V} -> V;
                        error -> { erlang:monitor(process, Pid), sets:new() }
                    end,
                NewLocksDict = dict:store(Pid, {Monitor, sets:union(PidKeySet, KeySet)}, LocksDict),
                NewLockedSet = sets:union(LockedSet, KeySet),
                {ok, State#state{ locked_set = NewLockedSet, locks = NewLocksDict }};
            false -> {failed, State}
        end,
    {reply, Rslt, NewState};

handle_call({unlock, KeySet}, {Pid, _Tag}, #state{ locked_set = LockedSet, locks = LocksDict } = State) ->
    {ok, {Monitor, PidKeySet}} = dict:find(Pid, LocksDict),
    NewPidKeySet = sets:subtract(PidKeySet, KeySet),
    NewLocksDict =
        case sets:size(NewPidKeySet) of
            0 -> 
                erlang:demonitor(Monitor, [flush]),
                dict:erase(Pid, LocksDict);
            _ ->
                dict:store(Pid, {Monitor, NewPidKeySet}, LocksDict)
        end,
    NewLockedSet = sets:subtract(LockedSet, KeySet), 
    {reply, ok, State#state{ locked_set = NewLockedSet, locks = NewLocksDict }};

handle_call(locked_keys, _From, #state{ locked_set = LockedSet } = State) ->
    {reply, {ok, LockedSet}, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Monitor, process, Pid, _Reason}, #state{ locked_set = LockedSet, locks = LocksDict } = State) ->
    {ok, {_, PidKeySet}} = dict:find(Pid, LocksDict),
    NewLockedSet = sets:subtract(LockedSet, PidKeySet),
    NewLocksDict = dict:erase(Pid, LocksDict),
    {noreply, State#state{ locked_set = NewLockedSet, locks = NewLocksDict }};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

with_priv(Table, KeyList, _KeySet, _Fun, Timeout, _MaxSleep)
        when Timeout < 0 ->
    throw({lock_failed, Table, KeyList});

with_priv(Table, KeyList, KeySet, Fun, Timeout, MaxSleep) ->
    case lock(Table, KeySet) of
        failed ->
            Sleep = random:uniform(MaxSleep),
            NewTimeout = Timeout - Sleep,
            if NewTimeout >= 0 -> timer:sleep(Sleep); true -> ok end,
            with_priv(Table, KeyList, KeySet, Fun, NewTimeout, MaxSleep);
        ok ->
            AL = [  case ets:lookup(Table, K) of
                        [] -> undefined;
                        [{K, V}] -> V
                    end || K <- KeyList ],
            try
                Update = Fun(AL),
                ets:insert(Table, lists:zip(KeyList, Update)),
                Update
            after
                unlock(Table, KeySet)
            end
    end.

lock(Table, KeySet) ->
    call(Table, {lock, KeySet}, ?LOCK_TIMEOUT).

unlock(Table, KeySet) ->
    call(Table, {unlock, KeySet}, ?UNLOCK_TIMEOUT).

call(Table, Query) ->
    gen_server:call(server_name(Table), Query).

call(Table, Query, Timeout) ->
    gen_server:call(server_name(Table), Query, Timeout).

server_name(Table) when is_atom(Table) ->
    % could be changed to list_to_atom("prfx_" ++ atom_to_list(Table))
    Table.
