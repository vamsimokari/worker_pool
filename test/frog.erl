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

-export([ribbet/1, ribbet/2, receive_ribbets/0]).

    
%%% Exported worker task functions

-type delay_ms()  :: pos_integer().
-type collector() :: pid().

-spec ribbet(delay_ms())              -> {pid(), ribbet}.
-spec ribbet(collector(), delay_ms()) -> {pid(), ribbet}.
-spec receive_ribbets()               -> done | timeout.

ribbet(Delay_Ms) ->
    Self = self(),
    timer:sleep(Delay_Ms),
    ct:log("Frog ~p woke up after ~pms delay", [Self, Delay_Ms]),
    {Self, ribbet}.

ribbet(Pid, Delay_Ms) ->
    Self = self(),
    timer:sleep(Delay_Ms),
    ct:log("Frog ~p woke up after ~pms delay", [Self, Delay_Ms]),
    Pid ! {Self, ribbet}.

receive_ribbets() ->
    receive
        stop -> done;
        {_Pid, ribbet} -> receive_ribbets()
    after 2000 -> timeout
    end.
