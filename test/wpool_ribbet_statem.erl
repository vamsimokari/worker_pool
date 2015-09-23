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
-export([legend/0, pretty_history/1, pretty_state/1]).


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
-type next_cmd()     :: task | pause.

-record(statem, {
          ribbet_collector          :: pid(),
          ribbet_pool               :: pid(),
          num_workers       =    0  :: worker_count(),
          available_workers =    0  :: worker_count(),
          busy_workers      =    0  :: worker_count(),
          pending_tasks     =    0  :: task_count()
         }).

-define(MAKE_POOL_CMD (__Num_Workers), {call, ?MODULE, make_pool, [frogs, __Num_Workers]}).
-define(TASK_LOW_CMD  (__Pid, __Min_Delay, __Max_Delay, __Num_Tasks, __Num_Workers),
        {call, frog, ribbet_low,  [__Pid, __Min_Delay, __Max_Delay, __Num_Tasks, __Num_Workers]}).
-define(TASK_HIGH_CMD (__Pid, __Min_Delay, __Max_Delay, __Num_Tasks, __Num_Workers),
        {call, frog, ribbet_high, [__Pid, __Min_Delay, __Max_Delay, __Num_Tasks, __Num_Workers]}).

%%% Uniquely mark the initial state.
initial_state() -> initial_state.


%%% Reflect the size of the worker pool when created.
next_state(initial_state, Var_Pool_Pid, ?MAKE_POOL_CMD(Num_Workers)) ->
    Collector_Pid = spawn(fun() -> frog:receive_ribbets() end),
    #statem{ribbet_collector  = Collector_Pid,
            ribbet_pool       = Var_Pool_Pid,
            num_workers       = Num_Workers,
            available_workers = Num_Workers};

%%% Check number of available workers and number of pending tasks.
next_state(#statem{num_workers=NW} = State, _Var, ?TASK_LOW_CMD  (_RC, _MinD, _MaxD, _Num_Tasks, NW)) -> State;
next_state(#statem{num_workers=NW} = State, _Var, ?TASK_HIGH_CMD (_RC, _MinD, _MaxD, _Num_Tasks, NW)) -> State.


%%% Create a new worker pool before issuing tasks to workers.
command(initial_state) ->
    ?MAKE_POOL_CMD(num_workers());

command(#statem{ribbet_collector=RC, num_workers=NW}) ->
    proper_types:oneof([
                        ?TASK_LOW_CMD  (RC, ribbet_delay_min(), ribbet_delay_max(), ribbet_few(NW),  NW),
                        ?TASK_HIGH_CMD (RC, ribbet_delay_min(), ribbet_delay_max(), ribbet_many(NW), NW)
                       ]).

precondition(_Current_State, _Call) -> true.

postcondition (initial_state,           ?MAKE_POOL_CMD (_Num_Workers), _Result) -> true;
postcondition (#statem{num_workers=NW}, ?TASK_LOW_CMD  (_RC, _MinD, _MaxD, _Num_Tasks, NW), _Result) -> verify_pool_stats(NW);
postcondition (#statem{num_workers=NW}, ?TASK_HIGH_CMD (_RC, _MinD, _MaxD, _Num_Tasks, NW), _Result) -> verify_pool_stats(NW).


%%% Total number of workers should match pool size, 0 pending tasks and no idle workers.
verify_pool_stats(Pool_Size) ->
    Pool_Stats = wpool_queue_manager:stats(frogs),
    Pool_Size  = proplists:get_value(pool_size, Pool_Stats),
    {Pool_Size, 0, 0}  = {proplists:get_value(available_workers, Pool_Stats),
                          proplists:get_value(pending_tasks,     Pool_Stats),
                          proplists:get_value(busy_workers,      Pool_Stats)},
    true.


%%% Datatype generators
num_workers  () -> proper_types:integer (  1,  30).
pause_delay  () -> proper_types:integer ( 30,  60).

%%% Ribbet delay allows variation, and time for pending tasks to build up.
ribbet_delay_min () -> proper_types:integer (  10, 100).
ribbet_delay_max () -> proper_types:integer ( 150, 300).

%%% Two different cases: 1) fewer tasks than workers; 2) more tasks than workers.
ribbet_few  (Num_Workers) -> proper_types:integer (               1,  max(Num_Workers - 1, 1)).
ribbet_many (Num_Workers) -> proper_types:integer ( Num_Workers + 1,          3 * Num_Workers).


%%% Pool utilities
-type timeout() :: non_neg_integer().

-spec make_pool(wpool:pool_name(), worker_count()) -> ok.
-spec make_pool(wpool:pool_name(), worker_count(), timeout(), string()) -> ok.

make_pool(Pool_Name, Num_Workers) ->
    comment_log("Creating pool ~p with ~p workers~n", [Pool_Name, Num_Workers]),
    start_pool(Pool_Name, Num_Workers, [{workers, Num_Workers}]).
    
make_pool(Pool_Name, Num_Workers, Timeout, Type_Of_Delay) ->
    comment_log("Creating pool ~p with ~p workers and ~pms ~s~n", [Pool_Name, Num_Workers, Timeout, Type_Of_Delay]),
    start_pool(Pool_Name, Num_Workers, [{workers, Num_Workers}]).

start_pool(Pool_Name, _Num_Workers, Options) ->
    {ok, Pool_Pid} = wpool:start_sup_pool(Pool_Name, Options),
    %% _Num_Workers = wpool_pool:wpool_size(Pool_Name),
    timer:sleep(100),   % Give some time for workers to start.
    Pool_Pid.


%%% Fancy reporting

-spec legend         ()       -> string().
-spec pretty_history (list()) -> list().
-spec pretty_state   (any())  -> any().

legend() ->
    "NW: Number of Workers; AW: Available; BW: Busy; PT: Pending Tasks".

pretty_history(History) ->
    [{pretty_state(State), Result} || {State, Result} <- History].

pretty_state(initial_state) -> initial_state;
pretty_state(#statem{num_workers=NW, available_workers=AW, busy_workers=BW, pending_tasks=PT}) ->
     lists:append([" NW:", integer_to_list(NW), " AW:", integer_to_list(AW),
                   " BW:", integer_to_list(BW), " PT:", integer_to_list(PT)]).

comment_log(Msg) ->
    comment_log(Msg, []).

comment_log(Msg, Args) ->
    ct:comment (Msg, Args),
    ct:log     (Msg, Args).
