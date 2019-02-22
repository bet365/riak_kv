%%%-------------------------------------------------------------------
%%% @author TiaShami
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Aug 2018 13:58
%%%-------------------------------------------------------------------
-module(riak_kv_get_helper_sup).
-author("mutiashami").
-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([start_vnode/3]).



start_vnode(riak_kv_get_helper, Index, ForwardTo) when is_integer(Index) ->
  supervisor_pre_r14b04:start_child(?MODULE, [riak_kv_get_helper, Index, ForwardTo]).
start_link() ->
  supervisor_pre_r14b04:start_link({local, ?MODULE}, ?MODULE, []).
init([]) ->
  {ok,
    {{simple_one_for_one, 10, 10},
      [{undefined,
        {riak_kv_get_helper, start_link, []},
        temporary, 300000, worker, dynamic}]}}.