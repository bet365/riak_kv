%%%-------------------------------------------------------------------
%%% @author mutiashami
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Jul 2018 14:09
%%%-------------------------------------------------------------------
-module(riak_kv_get_helper).
-author("mutiashami").
-behaviour(riak_core_vnode).
-include_lib("riak_core_pb.hrl").
-include("riak_kv_types.hrl").
-include_lib("riak_kv_vnode.hrl").
-include_lib("riak_kv_index.hrl").
%% API
-export([init/1,terminate/2,get/3]).
-export([start_vnode/1,
  start_vnodes/1,
  handle_overload_command/3,
  handle_command/3,
  delete/1,
  handle_handoff_command/3,
  encode_handoff_item/2,
  handle_coverage/4,
  handle_exit/3,
 handle_handoff_data/2,
  handoff_cancelled/1,
 handoff_finished/2,
  handoff_started/2,
  handoff_starting/2,
  is_empty/1,
  do_get/4,
  forward_get/3,
  handle_info/2,
  start_link/0]).

-define(DEFAULT_CNTR_LEASE_ERRS, 20).
-define(DEFAULT_CNTR_LEASE_TO, 20000).
-define(INDEX(A,B,C), _=element(1,{{_A1, _A2} = A,B,C}), ok).
-define(IS_SEARCH_ENABLED_FOR_BUCKET(BProps), _=element(1, {BProps}), false).
-define(MD_CACHE_BASE, "riak_kv_vnode_md_cache").
-record(counter_state, {
  use = true :: boolean(),
  cnt = 0 :: non_neg_integer(),
   lease = 0 :: non_neg_integer(),
  lease_size = 0 :: non_neg_integer(),

  leasing = false :: boolean()
}).

-record(state, {idx :: partition(),
  mod :: module(),
  modstate :: term(),
  backend_mode :: atom(),
  mrjobs :: term(),
  vnodeid :: undefined | binary(),
  delete_mode :: keep | immediate | pos_integer(),
  bucket_buf_size :: pos_integer(),
  index_buf_size :: pos_integer(),
  key_buf_size :: pos_integer(),
  async_folding :: boolean(),
  in_handoff = false :: boolean(),
  handoff_target :: node(),
  handoffs_rejected = 0 :: integer(),
  forward :: node() | [{integer(), node()}],
  hashtrees :: pid(),
  upgrade_hashtree = false :: boolean(),
  md_cache :: ets:tab(),
  md_cache_size :: pos_integer(),
  counter :: #counter_state{},
  status_mgr_pid :: pid() %% a process that manages vnode status persistence
}).
-define(MAX_CNTR_LEASE, 50000000).
-define(DEFAULT_CNTR_LEASE, 10000).
-define(ELSE, true).
-define(DEFAULT_HASHTREE_TOKENS, 90).
-ifdef(TEST).
-define(YZ_SHOULD_HANDOFF(X), true).
-else.
-define(YZ_SHOULD_HANDOFF(X), yz_kv:should_handoff(X)).
-endif.
-include_lib("bitcask/include/bitcask.hrl").


start_link() ->
  error_logger:info_msg("helper  inside  get helper  start link "),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, riak_kv_get_helper).

start_vnodes(IdxList) ->
    riak_core_vnode_master:get_vnode_pid(IdxList, riak_kv_get_helper).
init([Index]) ->
  error_logger:info_msg("helper  inside  get helper  init"),
  BackEndMode = read_only,
  Mod = app_helper:get_env(riak_kv, storage_backend),
  Configuration = app_helper:get_env(riak_kv),
  application:set_env(riak_kv, backend_mode, BackEndMode),
  BucketBufSize = app_helper:get_env(riak_kv, bucket_buffer_size, 1000),
  IndexBufSize = app_helper:get_env(riak_kv, index_buffer_size, 100),
  KeyBufSize = app_helper:get_env(riak_kv, key_buffer_size, 100),
  WorkerPoolSize = app_helper:get_env(riak_kv, worker_pool_size, 10),
  UseEpochCounter = app_helper:get_env(riak_kv, use_epoch_counter, true),
  CounterLeaseSize = min(?MAX_CNTR_LEASE,
    non_neg_env(riak_kv, counter_lease_size, ?DEFAULT_CNTR_LEASE)),
  {ok, StatusMgr} = riak_kv_vnode_status_mgr:start_link(self(), Index, UseEpochCounter),
  {ok, {VId, CounterState}} = get_vnodeid_and_counter(StatusMgr, CounterLeaseSize, UseEpochCounter),
  DeleteMode = app_helper:get_env(riak_kv, delete_mode, 3000),
  AsyncFolding = app_helper:get_env(riak_kv, async_folds, true) == true,
  MDCacheSize = app_helper:get_env(riak_kv, vnode_md_cache_size),
  MDCache =
    case MDCacheSize of
      N when is_integer(N),
        N > 0 ->
        lager:debug("Initializing metadata cache with size limit: ~p bytes",
          [MDCacheSize]),
        new_md_cache(VId);
      _ ->
        lager:debug("No metadata cache size defined, not starting"),
        undefined
    end,
  case catch Mod:start(Index, Configuration) of
    {ok, ModState} ->

      State = #state{idx=Index,
        async_folding=AsyncFolding,
        mod=Mod,
        modstate=ModState,
        backend_mode=BackEndMode,
        vnodeid=VId,
        counter=CounterState,
        status_mgr_pid=StatusMgr,
        delete_mode=DeleteMode,
        bucket_buf_size=BucketBufSize,
        index_buf_size=IndexBufSize,
        key_buf_size=KeyBufSize,
        mrjobs=dict:new(),
        md_cache=MDCache,
        md_cache_size=MDCacheSize},
      try_set_vnode_lock_limit(Index),
      case AsyncFolding of
        true ->
          %% Create worker pool initialization tuple
          FoldWorkerPool = {pool, riak_kv_worker, WorkerPoolSize, []},
          {ok, State, [FoldWorkerPool]};
        false ->
          {ok, State}
      end;
    {error, Reason} ->
      lager:error("Failed to start ~p backend for index ~p error: ~p",
        [Mod, Index, Reason]),
      riak:stop("backend module failed to start."),
      {error, Reason};
    {'EXIT', Reason1} ->
      lager:error("Failed to start ~p backend for index ~p crash: ~p",
        [Mod, Index, Reason1]),
      riak:stop("backend module failed to start."),
      {error, Reason1}
  end.


terminate(_Reason, #state{idx=Idx, mod=Mod, modstate=ModState,hashtrees=Trees}) ->
  Mod:stop(ModState),
  riak_kv_index_hashtree:sync_stop(Trees),
  riak_kv_stat:unregister_vnode_stats(Idx),
  ok.


get(Preflist, BKey, ReqId) ->
  get(Preflist, BKey, ReqId, {fsm, undefined, self()}).

get(Preflist, BKey, ReqId, Sender) ->
  Req = ?KV_GET_REQ{bkey=sanitize_bkey(BKey), req_id=ReqId},
  riak_core_vnode_master:command(Preflist, Req, Sender, riak_kv_get_helper_master).
sanitize_bkey({{<<"default">>, B}, K}) ->
  {B, K};
sanitize_bkey(BKey) ->
  BKey.

handle_overload_command(?KV_GET_REQ{req_id=ReqID}, Sender, Idx) ->
  riak_core_vnode:reply(Sender, {r, {error, overload}, Idx, ReqID}).
%%
handle_command(?KV_GET_REQ{bkey=BKey,req_id=ReqId},Sender,State) ->
  error_logger:info_msg("handle_command concurrent get"),
do_get(Sender, BKey, ReqId, State);
handle_command({backend_callback, Ref, Msg}, _Sender,
    State=#state{mod=Mod, modstate=ModState}) ->
  Mod:callback(Ref, Msg, ModState),
  {noreply, State}.
do_get(_Sender, BKey, ReqID,
    State=#state{idx=Idx, mod=Mod, modstate=ModState}) ->
    StartTS = os:timestamp(),
   {Retval, ModState1} = do_get_term(BKey, Mod, ModState),
  case Retval of
    {ok, Obj} ->
      maybe_cache_object(BKey, Obj, State);
    _ ->
      ok
  end,
  update_vnode_stats(vnode_get, Idx, StartTS),
  {reply, {r, Retval, Idx, ReqID}, State#state{modstate=ModState1}}.

do_get_term({Bucket, Key}, Mod, ModState) ->
  case do_get_object(Bucket, Key, Mod, ModState) of
    {ok, Obj, UpdModState} ->
      {{ok, Obj}, UpdModState};
    %% @TODO Eventually it would be good to
    %% make the use of not_found or notfound
    %% consistent throughout the code.
    {error, not_found, UpdModState} ->
      {{error, notfound}, UpdModState};
    {error, Reason, UpdModState} ->
      {{error, Reason}, UpdModState};
    Err ->
      Err
  end.

update_vnode_stats(Op, Idx, StartTS) ->
  ok = riak_kv_stat:update({Op, Idx, timer:now_diff( os:timestamp(), StartTS)}).
maybe_cache_object(BKey, Obj, #state{md_cache = MDCache,
  md_cache_size = MDCacheSize}) ->
  case MDCache of
    undefined ->
      ok;
    _ ->
      VClock = riak_object:vclock(Obj),
      IndexData = riak_object:index_data(Obj),
      insert_md_cache(MDCache, MDCacheSize, BKey, {VClock, IndexData})
  end.
insert_md_cache(Table, MaxSize, BKey, MD) ->
  TS = os:timestamp(),
  case ets:insert(Table, {TS, BKey, MD}) of
    true ->
      Size = ets:info(Table, memory),
      case Size > MaxSize of
        true ->
          trim_md_cache(Table, MaxSize);
        false ->
          ok
      end
  end.

trim_md_cache(Table, MaxSize) ->
  Oldest = ets:first(Table),
  case Oldest of
    '$end_of_table' ->
      ok;
    BKey ->
      ets:delete(Table, BKey),
      Size = ets:info(Table, memory),
      case Size > MaxSize of
        true ->
          trim_md_cache(Table, MaxSize);
        false ->
          ok
      end
  end.

do_get_object(Bucket, Key, Mod, ModState) ->
  case uses_r_object(Mod, ModState, Bucket) of
    true ->
      Mod:get_object(Bucket, Key, false, ModState);
    false ->
      case do_get_binary(Bucket, Key, Mod, ModState) of
        {ok, ObjBin, _UpdModState} ->
          BinSize = size(ObjBin),
          WarnSize = app_helper:get_env(riak_kv, warn_object_size),
          case BinSize > WarnSize of
            true ->
              lager:warning("Read large object ~p/~p (~p bytes)",
                [Bucket, Key, BinSize]);
            false ->
              ok
          end,
          try
            case riak_object:from_binary(Bucket, Key, ObjBin) of
              {error, Reason} ->
                throw(Reason);
              RObj ->
                {ok, RObj, _UpdModState}
            end
          catch _:_ ->
            lager:warning("Unreadable object ~p/~p discarded",
              [Bucket,Key]),
            {error, not_found, _UpdModState}
          end;
        Else ->
          Else
      end
  end.
do_get_binary(Bucket, Key, Mod, ModState) ->
  case uses_r_object(Mod, ModState, Bucket) of
    true ->
      error_logger:info_msg(" helper  true  Mod ~p ",[Mod]),
      Mod:get_object(Bucket, Key, true, ModState);
    false ->
      error_logger:info_msg(" helper  false Mod ~p ",[Mod]),
      Mod:get(Bucket, Key, ModState)
  end.
uses_r_object(Mod, ModState, Bucket) ->
  {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
  lists:member(uses_r_object, Capabilities).
%% -callbacks
%%handle_handoff_command(Req=?KV_PUT_REQ{}, Sender, State) ->
%%  ?KV_PUT_REQ{options=Options} = Req,
%%  case proplists:get_value(coord, Options, false) of
%%    false ->
%%      {noreply, NewState} = handle_command(Req, Sender, State),
%%      {forward, NewState};
%%    true ->
%%      %% riak_kv#1046 - don't make fake siblings. Perform the
%%      %% put, and create a new request to forward on, that
%%      %% contains the frontier, much like the value returned to
%%      %% a put fsm, then replicated.
%%      #state{idx=Idx} = State,
%%      ?KV_PUT_REQ{bkey=BKey,
%%        object=Object,
%%        req_id=ReqId,
%%        start_time=StartTime,
%%        options=Options} = Req,
%%      StartTS = os:timestamp(),
%%      riak_core_vnode:reply(Sender, {w, Idx, ReqId}),
%%      {Reply, UpdState} = do_put(Sender, BKey,  Object, ReqId, StartTime, Options, State),
%%      update_vnode_stats(vnode_put, Idx, StartTS),
%%
%%      case Reply of
%%        %%  NOTE: Coord is always `returnbody` as a put arg
%%        {dw, Idx, NewObj, ReqId} ->
%%          %% DO NOT coordinate again at the next owner!
%%          NewReq = Req?KV_PUT_REQ{options=proplists:delete(coord, Options),
%%            object=NewObj},
%%          {forward, NewReq, UpdState};
%%        _Error ->
%%          %% Don't forward a failed attempt to put, as you
%%          %% need the successful object
%%          {noreply, UpdState}
%%      end
%%  end;
%%handle_handoff_command(?KV_W1C_PUT_REQ{}=Request, Sender, State) ->
%%  NewState0 = case handle_command(Request, Sender, State) of
%%                {noreply, NewState} ->
%%                  NewState;
%%                {reply, Reply, NewState} ->
%%                  %% reply directly to the sender, as we will be forwarding the
%%                  %% the request on to the handoff node.
%%                  riak_core_vnode:reply(Sender, Reply),
%%                  NewState
%%              end,
%%  {forward, NewState0};
%%
%%%% Handle all unspecified cases locally without forwarding
%%handle_handoff_command(Req, Sender, State) ->
%%  handle_command(Req, Sender, State).
 handle_handoff_command(_Req,_Sender,State) ->
{noreply, State}.
delete(State=#state{status_mgr_pid=StatusMgr, mod=Mod, modstate=ModState}) ->
  %% clear vnodeid first, if drop removes data but fails
  %% want to err on the side of creating a new vnodeid
  {ok, cleared} = clear_vnodeid(StatusMgr),
  UpdModState = case Mod:drop(ModState) of
                  {ok, S} ->
                    S;
                  {error, Reason, S2} ->
                    lager:error("Failed to drop ~p. Reason: ~p~n", [Mod, Reason]),
                    S2
                end,
  case State#state.hashtrees of
    undefined ->
      ok;
    HT ->
      riak_kv_index_hashtree:destroy(HT)
  end,
  {ok, State#state{modstate=UpdModState,vnodeid=undefined,hashtrees=undefined}}.

encode_handoff_item({B, K}, V) ->

  ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
  try
    Value  = riak_object:to_binary_version(ObjFmt, B, K, V),
    encode_binary_object(B, K, Value)
  catch Error:Reason ->
    lager:warning("Handoff encode failed: ~p:~p",
      [Error,Reason]),
    corrupted
  end.
handle_coverage(?KV_LISTBUCKETS_REQ{item_filter=ItemFilter},
                _FilterVNodes,
                Sender,
                State=#state{async_folding=AsyncFolding,
                             bucket_buf_size=BufferSize,
                             mod=Mod,
                             modstate=ModState}) ->
    %% Construct the filter function
    Filter = riak_kv_coverage_filter:build_filter(ItemFilter),
    BufferMod = riak_kv_fold_buffer,

    Buffer = BufferMod:new(BufferSize, result_fun(Sender)),
    FoldFun = fold_fun(buckets, BufferMod, Filter, undefined),
    FinishFun = finish_fun(BufferMod, Sender),
    {ok, Capabilities} = Mod:capabilities(ModState),
    AsyncBackend = lists:member(async_fold, Capabilities),
    case AsyncFolding andalso AsyncBackend of
        true ->
            Opts = [async_fold];
        false ->
            Opts = []
    end,
    case list(FoldFun, FinishFun, Mod, fold_buckets, ModState, Opts, Buffer) of
        {async, AsyncWork} ->
            {async, {fold, AsyncWork, FinishFun}, Sender, State};
        _ ->
            {noreply, State}
    end;
handle_coverage(#riak_kv_listkeys_req_v3{bucket=Bucket,
                                         item_filter=ItemFilter},
                FilterVNodes, Sender, State) ->
    %% v3 == no backpressure
    ResultFun = result_fun(Bucket, Sender),
    Opts = [{bucket, Bucket}],
    handle_coverage_keyfold(Bucket, ItemFilter, ResultFun,
                            FilterVNodes, Sender, Opts, State);
handle_coverage(?KV_LISTKEYS_REQ{bucket=Bucket,
                                 item_filter=ItemFilter},
                FilterVNodes, Sender, State) ->
    %% v4 == ack-based backpressure
    ResultFun = result_fun_ack(Bucket, Sender),
    Opts = [{bucket, Bucket}],
    handle_coverage_keyfold(Bucket, ItemFilter, ResultFun,
                            FilterVNodes, Sender, Opts, State);
handle_coverage(#riak_kv_index_req_v1{bucket=Bucket,
                              item_filter=ItemFilter,
                              qry=Query},
                FilterVNodes, Sender, State) ->
    %% v1 == no backpressure
    handle_coverage_index(Bucket, ItemFilter, Query,
                          FilterVNodes, Sender, State, fun result_fun/2);
handle_coverage(?KV_INDEX_REQ{bucket=Bucket,
                              item_filter=ItemFilter,
                              qry=Query},
                FilterVNodes, Sender, State) ->
    %% v2 = ack-based backpressure
    handle_coverage_index(Bucket, ItemFilter, Query,
                          FilterVNodes, Sender, State, fun result_fun_ack/2).
handle_exit(Pid, Reason, State=#state{status_mgr_pid=Pid, idx=Index, counter=CntrState}) ->
  lager:error("Vnode status manager exit ~p", [Reason]),
  %% The status manager died, start a new one
  #counter_state{lease_size=LeaseSize, leasing=Leasing, use=UseEpochCounter} = CntrState,
  {ok, NewPid} = riak_kv_vnode_status_mgr:start_link(self(), Index, UseEpochCounter),

  if Leasing ->
    %% Crashed when getting a lease, try again pathalogical
    %% bad case here is that the lease gets to disk and the
    %% manager crashes, meaning an ever growing lease/new ids
    ok = riak_kv_vnode_status_mgr:lease_counter(NewPid, LeaseSize);
    ?ELSE ->
      ok
  end,
  {noreply, State#state{status_mgr_pid=NewPid}};
handle_exit(_Pid, Reason, State) ->
  %% A linked processes has died so the vnode
  %% process should take appropriate action here.
  %% The default behavior is to crash the vnode
  %% process so that it can be respawned
  %% by riak_core_vnode_master to prevent
  %% messages from stacking up on the process message
  %% queue and never being processed.
  lager:error("Linked process exited. Reason: ~p", [Reason]),
  {stop, linked_process_crash, State}.

handle_handoff_data(_BinObj, State) ->
              {reply, ok, State} .

%%    try
%%        {BKey, Val} = decode_binary_object(BinObj),
%%        {B, K} = BKey,
%%        case do_diffobj_put(BKey, riak_object:from_binary(B, K, Val),
%%                            State) of
%%            {ok, UpdModState} ->
%%                {reply, ok, State#state{modstate=UpdModState}};
%%            {error, Reason, UpdModState} ->
%%                {reply, {error, Reason}, State#state{modstate=UpdModState}}
%%        end
%%    catch Error:Reason2 ->
%%            lager:warning("Unreadable object discarded in handoff: ~p:~p",
%%                          [Error, Reason2]),
%%            {reply, ok, State}
%%    end.
handoff_cancelled(State) ->
    {ok, State#state{in_handoff=false, handoff_target=undefined}}.
handoff_started(_SrcPartition, _WorkerPid) ->
%%    case maybe_get_vnode_lock(SrcPartition, WorkerPid) of
%%        ok ->
            FoldOpts = [{iterator_refresh, true}],
            {ok, FoldOpts}.
%%        max_concurrency -> {error, max_concurrency}
%%    end.
handoff_finished(_TargetNode, State) ->
    {ok, State#state{in_handoff=false, handoff_target=undefined}}.
handoff_starting(_X, State) ->
  {true,State}.
%%handoff_starting({_HOType, TargetNode}=_X, State=#state{handoffs_rejected=RejectCount}) ->
%%  MaxRejects = app_helper:get_env(riak_kv, handoff_rejected_max, 6),
%%  case MaxRejects =< RejectCount orelse ?YZ_SHOULD_HANDOFF(_X) of
%%    true ->
%%      {true, State#state{in_handoff=true, handoff_target=TargetNode}};
%%    false ->
%%      {false, State#state{in_handoff=false, handoff_target=undefined, handoffs_rejected=RejectCount + 1 }}
%%  end.
is_empty(State=#state{mod=Mod, modstate=ModState}) ->
    IsEmpty = Mod:is_empty(ModState),
    case IsEmpty of
        true ->
            {true, State};
        false ->
            Size = maybe_calc_handoff_size(State),
            {false, Size, State}
    end.

non_neg_env(App, EnvVar, Default) when is_integer(Default),
                                       Default > 0 ->
    case app_helper:get_env(App, EnvVar, Default) of
        N when is_integer(N),
               N > 0 ->
            N;
        X ->
            lager:warning("Non-integer/Negative integer ~p for vnode counter config ~p."
                          " Using default ~p",
                          [X, EnvVar, Default]),
            Default
    end.

get_vnodeid_and_counter(StatusMgr, CounterLeaseSize, UseEpochCounter) ->
    {ok, {VId, Counter, Lease}} = riak_kv_vnode_status_mgr:get_vnodeid_and_counter(StatusMgr, CounterLeaseSize),
    {ok, {VId, #counter_state{cnt=Counter, lease=Lease, lease_size=CounterLeaseSize, use=UseEpochCounter}}}.

new_md_cache(VId) ->
  MDCacheName = list_to_atom(?MD_CACHE_BASE ++ integer_to_list(binary:decode_unsigned(VId))),
  %% ordered set to make sure that the first key is the oldest
  %% term format is {TimeStamp, Key, ValueTuple}
  ets:new(MDCacheName, [ordered_set, {keypos,2}]).
try_set_vnode_lock_limit(Idx) ->
    %% By default, register per-vnode concurrency limit "lock" with 1 so that only a
    %% single participating subsystem can run a vnode fold at a time. Participation is
    %% voluntary :-)
    Concurrency = case app_helper:get_env(riak_kv, vnode_lock_concurrency, 1) of
                      N when is_integer(N) -> N;
                      _NotNumber -> 1
                  end,
    try_set_concurrency_limit(?KV_VNODE_LOCK(Idx), Concurrency).

try_set_concurrency_limit(Lock, Limit) ->
    try_set_concurrency_limit(Lock, Limit, riak_core_bg_manager:use_bg_mgr()).

try_set_concurrency_limit(_Lock, _Limit, false) ->
    %% skip background manager
    ok;
try_set_concurrency_limit(Lock, Limit, true) ->
    %% this is ok to do more than once
    case riak_core_bg_manager:set_concurrency_limit(Lock, Limit) of
        unregistered ->
            %% not ready yet, try again later
            lager:debug("Background manager unavailable. Will try to set: ~p later.", [Lock]),
            erlang:send_after(250, ?MODULE, {set_concurrency_limit, Lock, Limit});
        _ ->
            lager:debug("Registered lock: ~p", [Lock]),
            ok
    end.
handle_coverage_keyfold(Bucket, ItemFilter, Query,
    FilterVNodes, Sender, State,
    ResultFunFun) ->
  handle_coverage_fold(fold_keys, Bucket, ItemFilter, Query,
    FilterVNodes, Sender, State, ResultFunFun).
handle_coverage_fold(FoldType, Bucket, ItemFilter, ResultFun,
                        FilterVNodes, Sender, Opts0,
                        State=#state{async_folding=AsyncFolding,
                                     idx=Index,
                                     key_buf_size=DefaultBufSz,
                                     mod=Mod,
                                     modstate=ModState}) ->
    %% Construct the filter function
    FilterVNode = proplists:get_value(Index, FilterVNodes),
    Filter = riak_kv_coverage_filter:build_filter(Bucket, ItemFilter, FilterVNode),
    BufferMod = riak_kv_fold_buffer,
    BufferSize = proplists:get_value(buffer_size, Opts0, DefaultBufSz),
    Buffer = BufferMod:new(BufferSize, ResultFun),
    Extras = fold_extras_keys(Index, Bucket),
    FoldFun = fold_fun(keys, BufferMod, Filter, Extras),
    FinishFun = finish_fun(BufferMod, Sender),
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    AsyncBackend = lists:member(async_fold, Capabilities),
    Opts = case AsyncFolding andalso AsyncBackend of
               true ->
                   [async_fold | Opts0];
               false ->
                   Opts0
           end,
    case list(FoldFun, FinishFun, Mod, FoldType, ModState, Opts, Buffer) of
        {async, AsyncWork} ->
            {async, {fold, AsyncWork, FinishFun}, Sender, State};
        _ ->
            {noreply, State}
   end.

clear_vnodeid(StatusMgr) ->
    riak_kv_vnode_status_mgr:clear_vnodeid(StatusMgr).
fold_fun(buckets, BufferMod, none, _Extra) ->
  fun(Bucket, Buffer) ->
    BufferMod:add(Bucket, Buffer)
  end;
fold_fun(buckets, BufferMod, Filter, _Extra) ->
  fun(Bucket, Buffer) ->
    case Filter(Bucket) of
      true ->
        BufferMod:add(Bucket, Buffer);
      false ->
        Buffer
    end
  end;
fold_fun(keys, BufferMod, none, undefined) ->
  fun(_, Key, Buffer) ->
    BufferMod:add(Key, Buffer)
  end;
fold_fun(keys, BufferMod, none, {Bucket, Index, N, NumPartitions}) ->
  fun(_, Key, Buffer) ->
    Hash = riak_core_util:chash_key({Bucket, Key}),
    case riak_core_ring:future_index(Hash, Index, N, NumPartitions, NumPartitions) of
      Index ->
        BufferMod:add(Key, Buffer);
      _ ->
        Buffer
    end
  end;
fold_fun(keys, BufferMod, Filter, undefined) ->
  fun(_, Key, Buffer) ->
    case Filter(Key) of
      true ->
        BufferMod:add(Key, Buffer);
      false ->
        Buffer
    end
  end;
fold_fun(keys, BufferMod, Filter, {Bucket, Index, N, NumPartitions}) ->
  fun(_, Key, Buffer) ->
    Hash = riak_core_util:chash_key({Bucket, Key}),
    case riak_core_ring:future_index(Hash, Index, N, NumPartitions, NumPartitions) of
      Index ->
        case Filter(Key) of
          true ->
            BufferMod:add(Key, Buffer);
          false ->
            Buffer
        end;
      _ ->
        Buffer
    end
  end.

encode_binary_object(Bucket, Key, Value) ->
    Method = handoff_data_encoding_method(),

    case Method of
        encode_raw  -> EncodedObject = { Bucket, Key, iolist_to_binary(Value) },
                       return_encoded_binary_object(Method, EncodedObject);

        %% zlib encoding is a special case, we return the legacy format:
        encode_zlib -> PBEncodedObject = riak_core_pb:encode_riakobject_pb(#riakobject_pb{bucket=Bucket, key=Key, val=Value}),
                       zlib:zip(PBEncodedObject)
    end.

%% Return objects in a consistent form:
return_encoded_binary_object(Method, EncodedObject) ->
    term_to_binary({ Method, EncodedObject }).
%% @private
result_fun(Sender) ->
    fun(Items) ->
            riak_core_vnode:reply(Sender, Items)
    end.

%% @private
result_fun(Bucket, Sender) ->
    fun(Items) ->
            riak_core_vnode:reply(Sender, {Bucket, Items})
    end.
finish_fun(BufferMod, Sender) ->
    fun(Buffer) ->
            finish_fold(BufferMod, Buffer, Sender)
    end.
result_fun_ack(Bucket, Sender) ->
    fun(Items) ->
            Monitor = riak_core_vnode:monitor(Sender),
            riak_core_vnode:reply(Sender, {{self(), Monitor}, Bucket, Items}),
            receive
                {Monitor, ok} ->
                    erlang:demonitor(Monitor, [flush]);
                {Monitor, stop_fold} ->
                    erlang:demonitor(Monitor, [flush]),
                    throw(stop_fold);
                {'DOWN', Monitor, process, _Pid, _Reason} ->
                    throw(receiver_down)
            end
    end.
fold_extras_keys(Index, Bucket) ->
  case app_helper:get_env(riak_kv, fold_preflist_filter, false) of
    true ->
      {ok, R} = riak_core_ring_manager:get_my_ring(),
      NValMap = nval_map(R),
      N = case lists:keyfind(Bucket, 1, NValMap) of
            false -> riak_core_bucket:default_object_nval();
            {Bucket, NVal} -> NVal
          end,
      NumPartitions = riak_core_ring:num_partitions(R),
      {Bucket, Index, N, NumPartitions};
    false ->
      undefined
  end.
finish_fold(BufferMod, Buffer, Sender) ->
    BufferMod:flush(Buffer),
    riak_core_vnode:reply(Sender, done).
handle_coverage_index(Bucket, ItemFilter, Query,
                      FilterVNodes, Sender,
                      State=#state{mod=Mod,
                                   key_buf_size=DefaultBufSz,
                                   modstate=ModState},
                      ResultFunFun) ->
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    IndexBackend = lists:member(indexes, Capabilities),
    case IndexBackend of
        true ->
            %% Update stats...
            ok = riak_kv_stat:update(vnode_index_read),

            ResultFun = ResultFunFun(Bucket, Sender),
            BufSize = buffer_size_for_index_query(Query, DefaultBufSz),
            Opts = [{index, Bucket, prepare_index_query(Query)},
                    {bucket, Bucket}, {buffer_size, BufSize}],
            %% @HACK
            %% Really this should be decided in the backend
            %% if there was a index_query fun.
            FoldType = case riak_index:return_body(Query) of
                           true -> fold_objects;
                           false -> fold_keys
                       end,
            handle_coverage_fold(FoldType, Bucket, ItemFilter, ResultFun,
                                    FilterVNodes, Sender, Opts, State);
        false ->
            {reply, {error, {indexes_not_supported, Mod}}, State}
    end.
list(FoldFun, FinishFun, Mod, ModFun, ModState, Opts, Buffer) ->
    case Mod:ModFun(FoldFun, Buffer, Opts, ModState) of
        {ok, Acc} ->
            FinishFun(Acc);
        {async, AsyncWork} ->
            {async, AsyncWork}
    end.

maybe_calc_handoff_size(#state{mod=Mod,modstate=ModState}) ->
  {ok, Capabilities} = Mod:capabilities(ModState),
  case lists:member(size, Capabilities) of
    true -> Mod:data_size(ModState);
    false -> undefined
  end.
handoff_data_encoding_method() ->
    riak_core_capability:get({riak_kv, handoff_data_encoding}, encode_zlib).

nval_map(Ring) ->
    riak_core_bucket:bucket_nval_map(Ring).
buffer_size_for_index_query(#riak_kv_index_v3{max_results=N}, DefaultSize)
  when is_integer(N), N < DefaultSize ->
    N;
buffer_size_for_index_query(_Q, DefaultSize) ->
    DefaultSize.
prepare_index_query(#riak_kv_index_v3{term_regex=RE} = Q) when
        RE =/= undefined ->
    {ok, CompiledRE} = re:compile(RE),
    Q#riak_kv_index_v3{term_regex=CompiledRE};
prepare_index_query(Q) ->
    Q.

handle_info({ensemble_get, Key, From}, State=#state{idx=Idx, forward=Fwd}) ->
    case Fwd of
        undefined ->
            {reply, {r, Retval, _, _}, State2} = do_get(undefined, Key, undefined, State),
            Reply = case Retval of
                        {ok, Obj} ->
                            Obj;
                        _ ->
                            notfound
                    end,
            riak_kv_ensemble_backend:reply(From, Reply),
            {ok, State2};
        Fwd when is_atom(Fwd) ->
            forward_get({Idx, Fwd}, Key, From),
            {ok, State}
    end;


handle_info({raw_forward_get, Key, From}, State) ->
  {reply, {r, Retval, _, _}, State2} = do_get(undefined, Key, undefined, State),
  Reply = case Retval of
            {ok, Obj} ->
              Obj;
            _ ->
              notfound
          end,
  riak_kv_ensemble_backend:reply(From, Reply),
  {ok, State2}.
forward_get({Idx, Node}, Key, From) ->
    Proxy = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx, Node),
    riak_core_send_msg:bang_unreliable(Proxy, {raw_forward_get, Key, From}),
    ok.