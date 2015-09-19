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

-module(wpool_proper_SUITE).
-author('jay@tigertext.com').

-export([
         all/0, groups/0,
         init_per_suite/1, end_per_suite/1,
         init_per_group/1, end_per_group/1
        ]).

%% Actual tests
-export([
         check_all_available_workers/1,
         check_pending_tasks/1
        ]).

-include("wpool_common_test.hrl").


%%% -----------------------------
%%% Init and setup functions
%%% -----------------------------

all() -> [{group, worker_counts}].

groups() -> [
             {worker_counts, [sequence],
              [{available_worker, [sequence],
                [
                 check_all_available_workers,
                 check_pending_tasks
                ]
               }]
             }
            ].

init_per_suite(Config) -> wpool:start(), Config.
end_per_suite(_Config) -> wpool:stop(),  ok.

init_per_group(Config) -> Config.
end_per_group(_Config) -> ok.


%%% -----------------------------
%%% PropEr Tests
%%% -----------------------------

-spec check_all_available_workers(config()) -> ok.
check_all_available_workers(_Config) ->

    comment_log("Has wpool_pool initialized properly?"),
    true = ets:info(wpool_pool, named_table),
    Frog_Pool_Name = frogs,

    comment_log("Fetch all available worker from pool to prove it works"),
    Test_Drain = ?FORALL({Num_Workers, Timeout}, {integer(1,30), integer(200,500)},
                         begin
                             ok = make_pool(Frog_Pool_Name, Num_Workers, Timeout, "fetch timeout"),
                             ok = drain_pool(Frog_Pool_Name, Num_Workers, Timeout),
                             ok =:= pending_task_cleanup(Frog_Pool_Name)
                         end
                        ),
    Num_Tests = 10,
    true = proper:quickcheck(Test_Drain, ?PQ_NUM(Num_Tests)),
    comment_log("Success with ~p all available worker checks", [Num_Tests]),
    ok.


-spec check_pending_tasks(config()) -> ok.
check_pending_tasks(_Config) ->

    comment_log("Has wpool_pool initialized properly?"),
    true = ets:info(wpool_pool, named_table),
    Frog_Pool_Name = frogs,

    comment_log("Simulate redis calls using redis_statem"),
    Sim_Module = wpool_ribbet_statem,
    Sim_Redis  = ?FORALL(Cmds, proper_statem:commands(Sim_Module),
                         ?TRAPEXIT(
                            begin
                                ct:log("Statem cmds: ~p", [Cmds]),
                                {History, State, Result} = proper_statem:run_commands(Sim_Module, Cmds),
                                pending_task_cleanup(Frog_Pool_Name),
                                   ct:log("~s~nHistory: ~p~nState: ~p~nResult: ~p~n",
                                          [Sim_Module:legend(),
                                           Sim_Module:pretty_history(History),
                                           Sim_Module:pretty_state(State),
                                           Result]),
                                ?WHENFAIL(
                                   ct:log("History: ~w~nState: ~w~nResult: ~p~n",
                                          [Sim_Module:pretty_history(History),
                                           Sim_Module:pretty_state(State),
                                           Result]),
                                   aggregate(command_names(Cmds), true))  %% Result =:= ok
                            end)),
    Num_Tests = 3,
    true = proper:quickcheck(Sim_Redis, ?PQ_NUM(Num_Tests)),
    comment_log("Success with ~p real work tests", [Num_Tests]),
    ok.

pending_task_cleanup(Pool_Name) ->
    ok = wpool:stop_pool(Pool_Name).


%%% -----------------------------
%%% Internal functions
%%% -----------------------------

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

drain_pool(Pool_Name, Num_Workers, Timeout) ->
    drain_pool(Pool_Name, Num_Workers, Timeout, []).

%%% Pool is now empty, verify the workers obtained.
drain_pool(Pool_Name, 0, _Timeout, Workers) ->

    %% See that all workers are busy after the pool is empty...
    Mgr_Stats_1 = wpool_queue_manager:stats(Pool_Name),
    ct:log("Workers all busy: ~p~n", [Mgr_Stats_1]),
    0  = proplists:get_value(pending_tasks,     Mgr_Stats_1),
    0  = proplists:get_value(available_workers, Mgr_Stats_1),
    BW = proplists:get_value(busy_workers,      Mgr_Stats_1),
    BW = length(Workers),

    %% Then return the workers and verify the pool is full.
    Mgr_Name = queue_manager_name(Pool_Name),
    [wpool_queue_manager:worker_ready(Mgr_Name, W) || W <- Workers, erlang:is_process_alive(whereis(W))],
    Mgr_Stats_2 = wpool_queue_manager:stats(Pool_Name),
    ct:log("Workers all available: ~p~n", [Mgr_Stats_2]),
    0  = proplists:get_value(pending_tasks,     Mgr_Stats_2),
    BW = proplists:get_value(available_workers, Mgr_Stats_2),
    0  = proplists:get_value(busy_workers,      Mgr_Stats_2),
    ok;

%%% Empty the pool collecting workers.
drain_pool(Pool_Name, Num_Workers, Timeout, Workers) ->
    try wpool_pool:available_worker(Pool_Name, Timeout) of
        Worker_Pid -> drain_pool(Pool_Name, Num_Workers-1, Timeout, [Worker_Pid | Workers])
    catch throw:no_workers -> no_workers
    end.

cast_work(Pool_Name, _Count, Cmd) ->
    wpool:cast(Pool_Name, Cmd).

%%% Internal support functions
comment_log(Msg) ->
    comment_log(Msg, []).

comment_log(Msg, Args) ->
    ct:comment (Msg, Args),
    ct:log     (Msg, Args).

queue_manager_name(Sup) -> list_to_atom("wpool_pool" ++ [$-|atom_to_list(Sup)] ++ "-queue-manager").
