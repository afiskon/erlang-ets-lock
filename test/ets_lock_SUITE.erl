-module(ets_lock_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        counters_test,
        lock_failed_test,
        locked_keys_test,
        inner_lock_test,
        double_lock_failed_test,
        monitor_test
    ].

counters_test(_Config) ->
    {ok, _} = ets_lock:start_link(aaa),
    ets_lock:with(aaa, [x, y], fun(_) -> [0, 0] end),
    Parent = self(),
    spawn_link(
        fun() ->
            [ ets_lock:with(aaa, [x], fun([X]) -> timer:sleep(10), [X+1] end) || I <- lists:seq(1, 100) ],
            Parent ! done
        end),
    spawn_link(
        fun() ->
            [ ets_lock:with(aaa, [y], fun([Y]) -> timer:sleep(10), [Y+1] end) || I <- lists:seq(1, 100) ],
            Parent ! done
        end),
    spawn_link(
        fun() ->
            [ ets_lock:with(aaa, [x, y], fun([X, Y]) -> timer:sleep(10), [X+1, Y+1] end) || I <- lists:seq(1, 100) ],
            Parent ! done
        end),
    spawn_link(
        fun() ->
            [ ets_lock:with(aaa, [y, x], fun([Y, X]) -> timer:sleep(10), [Y+1, X+1] end) || I <- lists:seq(1, 100) ],
            Parent ! done
        end),
    [ receive done -> ok end || I <- lists:seq(1,4) ],
    Result = ets_lock:with(aaa, [x, y], fun([X, Y]) -> [X, Y] end),
    ?assertEqual([300, 300], Result).

lock_failed_test(_Config) ->
    {ok, _} = ets_lock:start_link(aaa),
    spawn_link(
        fun() ->
            ets_lock:with(aaa, [y], fun(_) -> timer:sleep(5000), [1] end)
        end),
    timer:sleep(100),
    Result =
        try
            ets_lock:with(aaa, [x,y,z], fun(_) -> [1,2,3] end, 3000),
            ok
        catch
            _:Error -> Error
        end,
    ?assertEqual({lock_failed, aaa, [x,y,z]}, Result).

locked_keys_test(_Config) ->
    {ok, _} = ets_lock:start_link(aaa),
    spawn_link(fun() -> ets_lock:with(aaa, [a], fun(_) -> timer:sleep(1000), [1] end) end),
    spawn_link(fun() -> ets_lock:with(aaa, [b,c], fun(_) -> timer:sleep(1000), [2,3] end) end),
    spawn_link(fun() -> ets_lock:with(aaa, [d,e,f], fun(_) -> timer:sleep(1000), [4,5,6] end) end),
    ets_lock:with(aaa, [x,y,z], fun(_) -> [7,8,9] end),
    Result = ets_lock:locked_keys(aaa),
    ?assertEqual([a,b,c,d,e,f], lists:sort(Result)).

inner_lock_test(_Config) ->
    {ok, _} = ets_lock:start_link(aaa),
    ets_lock:with(aaa, [a,b,c],
        fun([A,B,C]) ->
            ets_lock:with(aaa, [d,e,f],
                fun([D,E,F]) ->
                    [4,5,6]
                end),
            [1,2,3]
        end),
    Result = ets_lock:with(aaa, [a,b,c,d,e,f], fun(Lst) -> Lst end),
    ?assertEqual([1,2,3,4,5,6], Result).

double_lock_failed_test(_Config) ->
    {ok, _} = ets_lock:start_link(aaa),
    Result =
        try
            ets_lock:with(aaa, [x,y,z],
                fun(Lst) ->
                    ets_lock:with(aaa, [z], fun([Z]) -> [Z] end),
                    Lst
                end),
            ok
        catch
            _:Error -> Error
        end,
    ?assertEqual({lock_failed, aaa, [z]}, Result).

monitor_test(_Config) ->
    {ok, _} = ets_lock:start_link(aaa),
    Pid = spawn(fun() -> ets_lock:with(aaa, [a,b,c], fun(_) -> timer:sleep(5000), [1,2,3] end) end),
    timer:sleep(100),
    exit(Pid, kill),
    timer:sleep(100),
    Result = ets_lock:locked_keys(aaa),
    ?assertEqual([], Result).
