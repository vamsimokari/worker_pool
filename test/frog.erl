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

-module(frog).
-author('jay@tigertext.com').

-export([
         %% ribbet/3,
         ribbet_low/5, ribbet_high/5,
         %% ribbet/4,
         receive_ribbets/0]).

    
%%% Exported worker task functions

-type delay_ms()    :: pos_integer().
-type num_tasks()   :: pos_integer().
-type num_workers() :: pos_integer().
-type collector()   :: pid().

%% -spec ribbet(delay_ms(), delay_ms(), num_tasks()) -> {pid(), ribbet}.

%% %%% No collector, no way to know when done...
%% ribbet(_Min_Delay, _Max_Delay,         0) -> ok;
%% ribbet( Min_Delay,  Max_Delay, Num_Tasks) ->
%%     _ = make_task(Min_Delay, Max_Delay),
%%     ribbet(Min_Delay, Max_Delay, Num_Tasks).

%% make_task(Min_Delay, Max_Delay) ->
%%     Delay = random:uniform(Min_Delay, Max_Delay),
%%     proc_lib:spawn_link(fun() -> task(Delay) end).

%% task(Delay_Ms) ->
%%     Self = self(),
%%     timer:sleep(Delay_Ms),
%%     ct:log("Frog ~p woke up after ~pms delay", [Self, Delay_Ms]),
%%     {Self, ribbet}.


-spec ribbet_low  (collector(), delay_ms(), delay_ms(), num_tasks(), num_workers()) -> {pid(), ribbet}.
-spec ribbet_high (collector(), delay_ms(), delay_ms(), num_tasks(), num_workers()) -> {pid(), ribbet}.
%% -spec ribbet      (collector(), delay_ms(), delay_ms(), num_tasks()) -> {pid(), ribbet}.

-spec receive_ribbets() -> done | timeout.


%%% External functions, differentiated in statem model.
ribbet_low(Collector_Pid, Min_Delay, Max_Delay, Num_Tasks, _Num_Workers)
  when Num_Tasks > 0 ->
    ct:log("Starting new set of frog tasks"),
    ribbet(self(), Collector_Pid, Min_Delay, Max_Delay, Num_Tasks),
    collect_ribbets(Collector_Pid, Max_Delay, Num_Tasks).

ribbet_high(Collector_Pid, Min_Delay, Max_Delay, Num_Tasks, Num_Workers)
  when Num_Tasks > 0 ->
    ct:log("Starting new set of frog tasks"),
    ribbet(self(), Collector_Pid, Min_Delay, Max_Delay, Num_Tasks),
    Total_Delay = ((Num_Tasks div Num_Workers) + 1) * Max_Delay,
    collect_ribbets(Collector_Pid, Total_Delay, Num_Tasks).

collect_ribbets(_Collector_Pid, _Max_Delay,         0) -> done;
collect_ribbets( Collector_Pid,  Max_Delay, Num_Tasks) ->
    receive {Collector_Pid, ribbet} -> collect_ribbets(Collector_Pid, Max_Delay, Num_Tasks-1)
    after   Max_Delay + 50          -> timeout
    end.


ribbet(_Parent, _Pid, _Min_Delay, _Max_Delay,         0) -> ok;
ribbet( Parent,  Pid,  Min_Delay,  Max_Delay, Num_Tasks) ->
    _ = make_task(Num_Tasks, Parent, Pid, Min_Delay, Max_Delay),
    ribbet(Parent, Pid, Min_Delay, Max_Delay, Num_Tasks-1).

make_task(Task_Num, Parent, Collector_Pid, Min_Delay, Max_Delay)
  when Min_Delay > 0 ->
    Spread = Max_Delay - Min_Delay + 1,
    Delay = random:uniform(Spread) + Min_Delay - 1,
    proc_lib:spawn_link(fun() -> task(Task_Num, Parent, Collector_Pid, Delay) end).

task(Task_Num, Parent, Collector_Pid, Delay_Ms) ->
    Self = self(),
    timer:sleep(Delay_Ms),
    ct:log("Frog #~p ~p woke up after ~pms delay", [Task_Num, Self, Delay_Ms]),
    Collector_Pid ! {Self, {tell_parent, Parent, ribbet}}.


%%% Collector action when receiving task completion message.
receive_ribbets() ->
    receive
        stop -> stopped;
        {_Pid, {tell_parent, Parent, ribbet}} ->
            Parent ! {self(), ribbet},
            receive_ribbets()
    after 2000 -> timeout
    end.
