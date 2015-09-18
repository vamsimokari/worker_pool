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

%% @hidden
-module(wpool_proper_SUITE).
-author('jay@duomark.com').

-export([
         all/0, groups/0,
         init_per_suite/1, end_per_suite/1,
         init_per_group/1, end_per_group/1
        ]).

%% Actual tests
-export([
         check_idle_workers/1
        ]).

%% Worker tasks.
-export([ribbet/0]).

-include("wpool_common_test.hrl").

all() -> [{group, worker_counts}].

groups() -> [
             {worker_counts, [sequence],
              [{available_worker, [sequence],
                [
                 check_idle_workers
                ]
               }]
             }
            ].

init_per_suite(Config) ->
    wpool:start(),
    Config.
end_per_suite(_Config) ->
    wpool:stop(),
    ok.

init_per_group(Config) ->
    Config.
end_per_group(_Config) ->
    ok.

-spec check_idle_workers(config()) -> ok.
check_idle_workers(_Config) ->

    comment_log("Has wpool_pool initialized properly?"),
    true = ets:info(wpool_pool, named_table),
    Frog_Pool_Name = frogs,

    comment_log("Fetch all available worker from pool to prove it works"),
    Test_Idle = ?FORALL({Num_Workers, Timeout}, {integer(1,5), integer(3000,10000)},
                        begin
                            ok = make_pool(Frog_Pool_Name, Num_Workers),
                            ok = drain_pool(Frog_Pool_Name, Num_Workers, Timeout),
                            ok =:= wpool:stop_pool(Frog_Pool_Name)
                        end
                       ),
    Num_Tests = 10,
    true = proper:quickcheck(Test_Idle, ?PQ_NUM(Num_Tests)),
    comment_log("Success with ~p idle worker checks", [Num_Tests]),
    ok.


make_pool(Pool_Name, Num_Workers) ->
    comment_log("Creating pool ~p with ~p workers", [Pool_Name, Num_Workers]),
    {ok, _Pool_Pid} = wpool:start_sup_pool(Pool_Name, [{workers, Num_Workers}]),
    %% comment_log("Wpool_pool ~p has ~p workers", [Pool_Name, wpool_pool:wpool_size(Pool_Name)]),
    Num_Workers = wpool_pool:wpool_size(Pool_Name),
    ok.

drain_pool(Pool_Name, Num_Workers, Timeout) ->
    drain_pool(Pool_Name, Num_Workers, Timeout, []).

%%% Pool is now empty, verify the workers obtained.
drain_pool(Pool_Name, 0, _Timeout, Workers) ->

    %% See that all workers are busy after the pool is empty...
    Mgr_Stats_1 = wpool_queue_manager:stats(Pool_Name),
    ct:log("Stats 1: ~p~n", [Mgr_Stats_1]),
    0  = proplists:get_value(pending_tasks,     Mgr_Stats_1),
    0  = proplists:get_value(available_workers, Mgr_Stats_1),
    BW = proplists:get_value(busy_workers,      Mgr_Stats_1),
    BW = length(Workers),

    %% Then return the workers and verify the pool is full.
    Mgr = Pool_Name,
    [wpool_queue_manager:worker_ready(Mgr, W) || W <- Workers],
    timer:sleep(100),
    Mgr_Stats_2 = wpool_queue_manager:stats(Pool_Name),
    ct:log("Stats 2: ~p~n", [Mgr_Stats_2]),
    0  = proplists:get_value(pending_tasks,     Mgr_Stats_2),
    BW = proplists:get_value(available_workers, Mgr_Stats_2),
    0  = proplists:get_value(busy_workers,      Mgr_Stats_2),
    ok;

%%% Empty the pool collecting workers.
drain_pool(Pool_Name, Num_Workers, Timeout, Workers) ->
    try wpool_pool:available_worker(Pool_Name, infinity) of
        Worker_Pid -> drain_pool(Pool_Name, Num_Workers-1, Timeout, [Worker_Pid | Workers])
    catch throw:no_workers -> no_workers
    end.


%%% Exported worker task functions
-spec ribbet() -> ribbet.
ribbet() ->
    timer:sleep(random:uniform(200)),
    ribbet.

%%% Internal support functions
comment_log(Msg) ->
    comment_log(Msg, []).

comment_log(Msg, Args) ->
    ct:comment (Msg, Args),
    ct:log     (Msg, Args).

