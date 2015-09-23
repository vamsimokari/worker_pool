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
%% @doc Default instance for {@link wpool_process}
-module(wpool_worker).
-author('elbrujohalcon@inaka.net').

-behaviour(gen_server).

%% api
-export([call/4, cast/4]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, handle_info/2]).

-define(IDLE_TIMEOUT, 1000).

%%%===================================================================
%%% API
%%%===================================================================
%% @doc Returns the result of M:F(A) from any of the workers of the pool S
-spec call(wpool:name(), module(), atom(), [term()]) -> term().
call(S, M, F, A) ->
  case wpool:call(S, {M,F,A}) of
    {ok, Result} -> Result;
    {error, Error} -> throw(Error)
  end.

%% @doc Executes M:F(A) in any of the workers of the pool S
-spec cast(wpool:name(), module(), atom(), [term()]) -> ok.
cast(S, M, F, A) ->
  wpool:cast(S, {M,F,A}).

%%%===================================================================
%%% init, terminate, code_change, info callbacks
%%%===================================================================

-record(state, {hibernate = always :: always | never | when_idle}).

-type from()    :: {pid(), reference()}.

-type reply()   :: {reply, {ok, term()} | {error, term()}, #state{}}
                 | {reply, {ok, term()} | {error, term()}, #state{}, hibernate}.

-type noreply() :: {noreply, #state{}}
                 | {noreply, #state{}, hibernate | pos_integer()}.

%% @private
-spec init(proplists:proplist()) -> {ok, #state{}}.
init(Options) ->
    case proplists:get_value(hibernate, Options, always) of
        never     -> {ok, #state{hibernate = never}};
        always    -> {ok, #state{hibernate = always}, hibernate};
        when_idle -> {ok, #state{hibernate = when_idle}, ?IDLE_TIMEOUT}
    end.
%% @private
-spec terminate(atom(), #state{}) -> ok.
terminate(_Reason, _State) -> ok.
%% @private
-spec code_change(string(), #state{}, any()) -> {ok, {}}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
%% @private
-spec handle_info(any(), #state{}) -> noreply().
handle_info(timeout, State) -> noreply(State, hibernate);
handle_info(_Info,   State) -> noreply(State).

%%%===================================================================
%%% real (i.e. interesting) callbacks
%%%===================================================================
%% @private
-spec handle_cast(term(), #state{}) -> noreply().
handle_cast({M,F,A}, State) ->
  try erlang:apply(M, F, A)
  catch
    _:Error ->
      LogArgs = [M, F, A, Error, erlang:get_stacktrace()],
      lager:error("Error on ~p:~p~p >> ~p Backtrace ~p", LogArgs)
  end,
  noreply(State);
handle_cast(Cast, State) ->
  lager:error("Invalid cast:~p", [Cast]),
  noreply(State).

%% @private
-spec handle_call(term(), from(), #state{}) -> reply().
handle_call({M,F,A}, _From, State) ->
  try erlang:apply(M, F, A) of
    R -> reply(State, {ok, R})
  catch
    _:Error ->
      LogArgs = [M, F, A, Error, erlang:get_stacktrace()],
      lager:error("Error on ~p:~p~p >> ~p Backtrace ~p", LogArgs),
      reply(State, {error, Error})
  end;
handle_call(Call, _From, State) ->
  lager:error("Invalid call:~p", [Call]),
  reply(State, {error, invalid_request}).

%%% reply/1 and noreply/1,2 are used exclusively so that calls can be traced more easily.
reply( #state{hibernate = never     } = State, Reply) -> {reply, Reply, State};
reply( #state{hibernate = always    } = State, Reply) -> {reply, Reply, State, hibernate};
reply( #state{hibernate = when_idle } = State, Reply) -> {reply, Reply, State, ?IDLE_TIMEOUT}.
        
noreply( #state{hibernate = never     } = State) -> {noreply, State};
noreply( #state{hibernate = always    } = State) -> {noreply, State, hibernate};
noreply( #state{hibernate = when_idle } = State) -> {noreply, State, ?IDLE_TIMEOUT}.

noreply( #state{} = State, hibernate ) -> {noreply, State, hibernate}.
