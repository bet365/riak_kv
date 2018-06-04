%%%-------------------------------------------------------------------
%%% @author nordinesaabouni
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. May 2018 13:16
%%%-------------------------------------------------------------------
-module(riak_core_optimised_apl).
-author("nordinesaabouni").

-behaviour(gen_server).

%% API
-export(
[
    start_link/0,
    update_responsiveness_measurement/4
]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    table
}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

update_responsiveness_measurement(Code, Idx, StartTime, Endtime) ->
    gen_server:cast(?SERVER, {update, Code, Idx, StartTime, Endtime}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    Table = ets:new(responsive_tab, [named_table, protected, ordered_set]),
    {ok, #state{table = Table}}.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
