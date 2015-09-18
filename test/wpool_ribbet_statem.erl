% This file is licensed to you under the Apache License,
% Version 2.0 (the "License"); you may not use this file
% except in compliance with the License.  You may obtain
% a copy of the License at
%
% http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing,
% software distributed under the License is distributed on an
% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
% KIND, either express or implied.  See the License for the
% specific language governing permissions and limitations
% under the License.

-module(wpool_ribbet_statem).
-author('jay@tigertext.com').

-export([initial_state/0, command/1,
         precondition/2, postcondition/3,
         next_state/3]).

-export([make_pool/2, make_pool/4]).
-export([pretty_history/1, pretty_state/1]).


%%% Types used in generating statem state transitions
-type statem_state() :: proplists:proplist().
-type call()         :: {call, Timeout::pos_integer()}.
-type result()       :: term().

%%% Required behaviour for statem to generate and execute tests
-spec initial_state ()                                 -> [].
-spec command       (statem_state())                   -> call().
-spec precondition  (statem_state(), call())           -> boolean().
-spec postcondition (statem_state(), call(), result()) -> boolean().
-spec next_state    (statem_state(), result(), call()) -> statem_state().

%%% Internal state of the testing state machine
-type task_count()   :: non_neg_integer().
-type worker_count() :: non_neg_integer().

-record(statem, {
          ribbet_collector      :: pid(),
          num_workers       = 0 :: worker_count(),
          available_workers = 0 :: worker_count(),
          busy_workers      = 0 :: worker_count(),
          pending_tasks     = 0 :: task_count()
         }).

-define(MAKE_POOL_CMD   (__Num_Workers),  {call, ?MODULE, make_pool, [frogs, __Num_Workers]}).
-define(TASK_WORKER_CMD (__Pid, __Delay), {call, frog,    ribbet,    [__Pid, __Delay]}).

%%% Uniquely mark the initial state.
initial_state() -> initial_state.


%%% Reflect the size of the worker pool when created.
next_state(initial_state, _Var, ?MAKE_POOL_CMD(Num_Workers)) ->
    Collector_Pid = spawn(fun() -> frog:receive_ribbets() end),
    #statem{ribbet_collector  = Collector_Pid,
            num_workers       = Num_Workers,
            available_workers = Num_Workers};

%%% Deduct from the available workers when tasks are executed.
next_state(#statem{available_workers=AW, busy_workers=BW} = State,
           _Var, ?TASK_WORKER_CMD(_RC, _Delay)) ->
    State#statem{available_workers = AW-1,
                 busy_workers      = BW+1}.


%%% Create a new worker pool before issuing tasks to workers.
command(initial_state)                -> ?MAKE_POOL_CMD   (num_workers());
command(#statem{ribbet_collector=RC}) -> ?TASK_WORKER_CMD (RC, ribbet_delay()).

precondition  (_Current_State, _Call)          -> true.
postcondition (_Prior_State,   _Call, _Result) -> true.


%%% Datatype generators

num_workers  () -> proper_types:integer (  1,  30).
ribbet_delay () -> proper_types:integer ( 20, 300).


%%% Pool utilities

-type timeout() :: non_neg_integer().

-spec make_pool(wpool:pool_name(), worker_count()) -> ok.
-spec make_pool(wpool:pool_name(), worker_count(), timeout(), string()) -> ok.

make_pool(Pool_Name, Num_Workers) ->
    comment_log("Creating pool ~p with ~p workers", [Pool_Name, Num_Workers]),
    start_pool(Pool_Name, Num_Workers, [{workers, Num_Workers}]).
    
make_pool(Pool_Name, Num_Workers, Timeout, Type_Of_Delay) ->
    comment_log("Creating pool ~p with ~p workers and ~pms ~s", [Pool_Name, Num_Workers, Timeout, Type_Of_Delay]),
    start_pool(Pool_Name, Num_Workers, [{workers, Num_Workers}]).

start_pool(Pool_Name, Num_Workers, Options) ->
    {ok, _Pool_Pid} = wpool:start_sup_pool(Pool_Name, Options),
    Num_Workers = wpool_pool:wpool_size(Pool_Name),
    ok.


%%% Fancy reporting

-spec pretty_history (list()) -> list().
-spec pretty_state   (any())  -> any().

pretty_history(History) ->
    [{pretty_state(State), Result} || {State, Result} <- History].

pretty_state(initial_state) -> initial_state;
pretty_state(#statem{num_workers=NW, available_workers=AW, busy_workers=BW, pending_tasks=PT}) ->
    lists:append([ "NW:", integer_to_list(NW), " AW:", integer_to_list(AW),
                  " BW:", integer_to_list(BW), " PT:", integer_to_list(PT)]).

comment_log(Msg) ->
    comment_log(Msg, []).

comment_log(Msg, Args) ->
    ct:comment (Msg, Args),
    ct:log     (Msg, Args).
