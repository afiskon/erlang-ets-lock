erlang-ets-lock
===============

Simple ETS Locks Manager

Usage:

```
$ rebar compile ct
$ erl -pa ./ebin/

1> l(ets_lock).
{module,ets_lock}
2> ets_lock:start_link(eax_me).
{ok,<0.36.0>}
3> ets_lock:with(eax_me, [x,y,z], fun(_) -> [0,0,0] end).
[0,0,0]
4> [ ets_lock:with(
       eax_me, [x,y,z],
       fun([X,Y,Z]) -> [X+1, Y+1, Z+1] end)
     || I <- lists:seq(1,1000) ], ok.
ok
5> ets:tab2list(eax_me).
[{z,1000},{y,1000},{x,1000}]
```
