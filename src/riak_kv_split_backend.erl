%%%-------------------------------------------------------------------
%%% @author dylanmitelo
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. May 2019 09:37
%%%-------------------------------------------------------------------
%% -------------------------------------------------------------------
%%
%% riak_split_backend: switching between multiple storage engines
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc riak_kv_split_backend allows you to run multiple backends within a
%% single Riak instance. The 'backend' property of a bucket specifies
%% the backend in which the object should be stored. If no 'backend'
%% is specified, then the 'split_backend_default' setting is used.
%% If this is unset, then the first defined backend is used.
%%
%% === Configuration ===
%%
%% ```
%%     {storage_backend, riak_kv_split_backend},
%%     {split_backend_default, first_backend},
%%     {split_backend, [
%%       % format: {name, module, [Configs]}
%%       {first_backend, riak_xxx_backend, [
%%         {config1, ConfigValue1},
%%         {config2, ConfigValue2}
%%       ]},
%%       {second_backend, riak_yyy_backend, [
%%         {config1, ConfigValue1},
%%         {config2, ConfigValue2}
%%       ]}
%%     ]}
%% '''
%%
%% Then, tell a bucket which one to use...
%%
%% ```
%%     riak_core_bucket:set_bucket(<<"MY_BUCKET">>, [{backend, second_backend}])
%% '''
%%
-module(riak_kv_split_backend).
-author("dylanmitelo").
-behavior(riak_kv_backend).

%% KV Backend API
-export([api_version/0,
	capabilities/1,
	capabilities/2,
	start/2,
	start_additional_backends/3,
	check_backend_exists/2,
	stop/1,
	get/3,
	put/5,
	put/6,
	delete/4,
	drop/1,
	drop_backend/2,
	fold_buckets/4,
	fold_keys/4,
	fold_objects/4,
	fold_indexes/4,
	is_empty/1,
	data_size/1,
	status/1,
	callback/3,
	fix_index/3,
	set_legacy_indexes/2,
	mark_indexes_fixed/2,
	fixed_index_status/1,
	transfer_to_splits/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("bitcask/include/bitcask.hrl").
-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold, index_reformat]).
-define(ANY_CAPABILITIES, [indexes, iterator_refresh, backend_reap]).
-define(VERSION_1, 1).
-define(VERSION_2, 2).
-define(VERSION_BYTE, ?VERSION_2).

-record (state, {backends :: [{atom(), atom(), term()}], % [{BackendName, BackendModule, SubState}]
	default_backend :: atom()}).

-type state() :: #state{}.
-type config() :: [{atom(), term()}].

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the
%% current API.
-spec api_version() -> {ok, integer()}.
api_version() ->
	{ok, ?API_VERSION}.

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(State) ->
	%% Expose ?CAPABILITIES plus the intersection of all child
	%% backends. (This backend creates a shim for any backends that
	%% don't support async_fold.)
	F = fun(Mod, ModState) ->
		{ok, S1} = Mod:capabilities(ModState),
		ordsets:from_list(S1)
		end,
	AllCaps = [F(Mod, ModState) || {_, Mod, ModState} <- State#state.backends],
	lager:info("Capabilities state backend: ~p~n", [State#state.backends]),
	Caps1 = ordsets:intersection(AllCaps),
	Caps2 = ordsets:to_list(Caps1),

	% Some caps we choose if ANY backend has them
	AnyCaps = ordsets:intersection(ordsets:union(AllCaps),
		ordsets:from_list(?ANY_CAPABILITIES)),
	Capabilities = lists:usort(?CAPABILITIES ++ Caps2 ++ AnyCaps),
	{ok, Capabilities}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(Bucket, #state{default_backend = DefBucket} = State) when is_binary(Bucket) ->
	case get_backend(DefBucket, State) of
		{error, State} = Error ->
			Error;
		{_Name, Mod, ModState} ->
			Mod:capabilities(ModState)
	end;
capabilities(_Bucket, State) ->
	capabilities(State).

%% @doc Start the backends
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
	%% Sanity checking
	Defs =  app_helper:get_prop_or_env(split_backend, Config, riak_kv),
	if Defs =:= undefined ->
		{error, split_backend_config_unset};
		not is_list(Defs) ->
			{error, {invalid_config_setting,
				split_backend,
				list_expected}};
		length(Defs) =< 0 ->
			{error, {invalid_config_setting,
				split_backend,
				list_is_empty}};
		true ->
			%% Get the default
			{First, _Mod, _ModConf} = hd(Defs),
			DefaultName = app_helper:get_prop_or_env(split_backend_default, Config, riak_kv, First),

			case lists:keyfind(DefaultName, 1, Defs) of
				{DefaultName, Mod, _ModConf1} ->
					Final = fetch_metadata_backends(Defs),
					metadata_config_puts(First, Mod),
					lager:info("Split Backend Starting with Config: ~p and Metadata values: ~p~n", [Defs, Final]),

					%% Start the backends
					BackendFun = start_backend_fun(Partition),
					{Backends, Errors} =
						lists:foldl(BackendFun, {[], []}, Final),
					case Errors of
						[] ->
							{ok, #state{backends=Backends,
								default_backend= DefaultName}};
						_ ->
							{error, Errors}
					end;
				false ->
					{error, {invalid_config_setting,
						split_backend_default,
						backend_not_found}}
			end
	end.

start_additional_backends(_Partition, Config, _State) when undefined =:= Config ->
	error;
start_additional_backends(Partition, Config, State) when is_tuple(Config) ->
	start_additional_backends(Partition, [Config], State);
start_additional_backends(Partition, Config, State = #state{backends = CurrentBackends}) ->
	lager:info("Starting Additional backend on partition: ~p with config: ~p~n", [Partition, Config]),
	BackendFun = start_backend_fun(Partition),
	{Backends, Errors} =
		lists:foldl(BackendFun, {[], []}, Config),
	case Errors of
		[] ->
			lager:info("Additional backend has started: ~p~n", [Partition]),
			{ok, State#state{backends=lists:flatten([Backends | CurrentBackends])}};
		_ ->
			{error, Errors}
	end.

%% @private
start_backend_fun(Partition) ->
	fun({Name, Module, ModConfig}, {Backends, Errors}) ->
		try
			case start_backend(Name,
				Module,
				Partition,
				ModConfig) of
				{Module, Reason} ->
					{Backends,
						[{Module, Reason} | Errors]};
				Backend ->
					{[Backend | Backends],
						Errors}
			end
		catch _:Error ->
			{Backends,
				[{Module, Error} | Errors]}
		end
	end.

%% @private
start_backend(Name, Module, Partition, Config) ->
	try
		case Module:start(Partition, Config) of
			{ok, State} ->
				{Name, Module, State};
			{error, Reason} ->
				{Module, Reason}
		end
	catch
		_:Reason1 ->
			{Module, Reason1}
	end.

check_backend_exists(Key, #state{backends = CurrentBackends, default_backend = DefaultName}) ->
	lager:info("Looking for Key: ~p Current Backends: ~p~n", [Key, CurrentBackends]),
	case lists:keyfind(Key, 1, CurrentBackends) of
		false ->
			{DefaultName, Mod, ModState} = lists:keyfind(DefaultName, 1, CurrentBackends),
			lager:info("Default name: ~p and ModState to search: ~p~n", [DefaultName, ModState]),
			FoldFun = fun(Bucket, Acc) -> lager:info("FoldBucket, Fun, Bucket: ~p and Acc~p~n", [Bucket, Acc]), [Bucket | Acc] end,
			{ok, Buckets} = Mod:fold_buckets(FoldFun, [], [], ModState),
			lager:info("Buckets: ~p~n", [Buckets]),
			case lists:keyfind(Key, 1, Buckets) of
				false ->
					false;
				true ->
					true
			end;
		_ ->
			true
	end.


%% @doc Stop the backends
-spec stop(state()) -> ok.
stop(#state{backends=Backends}) ->
	_ = [Module:stop(SubState) || {_, Module, SubState} <- Backends],
	ok.

%% @doc Retrieve an object from the backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
	{ok, any(), state()} |
	{ok, not_found, state()} |
	{error, term(), state()}.
get(Bucket, Key, State) ->
	case get_backend(Bucket, State) of
		{error, State1} ->
			{error, not_found, State1};
		{Name, Module, SubState} ->
			case Module:get(Bucket, Key, SubState) of
				{ok, Value, NewSubState} ->
					NewState = update_backend_state(Name, Module, NewSubState, State),
					{ok, Value, NewState};
				{error, Reason, NewSubState} ->
					NewState = update_backend_state(Name, Module, NewSubState, State),
					{error, Reason, NewState}
			end
	end.

%% @doc Insert an object with secondary index
%% information into the kv backend
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), integer(), state()) ->
	{ok, state()} |
	{error, term(), state()}.
put(Bucket, PrimaryKey, IndexSpecs, Value, TimeStampExpire, State) ->
	case get_backend(Bucket, State) of
		{error, State1} ->
			{error, "Put rejected due to no backend being configured", State1};
		{Name, Module, SubState} ->
			case Module:put(Bucket, PrimaryKey, IndexSpecs, Value, TimeStampExpire, SubState) of
				{ok, NewSubState} ->
					NewState = update_backend_state(Name, Module, NewSubState, State),
					{ok, NewState};
				{error, Reason, NewSubState} ->
					NewState = update_backend_state(Name, Module, NewSubState, State),
					{error, Reason, NewState}
			end
	end.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
	{ok, state()} |
	{error, term(), state()}.
put(Bucket, PrimaryKey, IndexSpecs, Value, State) ->
	case get_backend(Bucket, State) of
		{error, State1} ->
			{error, "Put rejected due to no backend being configured", State1};
		{Name, Module, SubState} ->
			case Module:put(Bucket, PrimaryKey, IndexSpecs, Value, SubState) of
				{ok, NewSubState} ->
					NewState = update_backend_state(Name, Module, NewSubState, State),
					{ok, NewState};
				{error, Reason, NewSubState} ->
					NewState = update_backend_state(Name, Module, NewSubState, State),
					{error, Reason, NewState}
			end
	end.

%% @doc Delete an object from the backend
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
	{ok, state()} |
	{error, term(), state()}.
delete(Bucket, Key, IndexSpecs, State) ->
	case get_backend(Bucket, State) of
		{error, State1} ->
			lager:error("Failed Split backend delete due to no backend for Bucket: ~p existing", [Bucket]),
			{error, "No backend configured to delete object from", State1};
		{Name, Module, SubState} ->
			case Module:delete(Bucket, Key, IndexSpecs, SubState) of
				{ok, NewSubState} ->
					NewState = update_backend_state(Name, Module, NewSubState, State),
					{ok, NewState};
				{error, Reason, NewSubState} ->
					NewState = update_backend_state(Name, Module, NewSubState, State),
					{error, Reason, NewState}
			end
	end.

%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
	any(),
	[{atom(), term()}],
	state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_buckets(FoldBucketsFun, Acc, Opts, State) ->
	fold_all(fold_buckets, FoldBucketsFun, Acc, Opts, State,
		fun default_backend_filter/5).

%% @doc Fold only over index data in the backend, for all buckets.
fold_indexes(FoldIndexFun, Acc, Opts, State) ->
	fold_all(fold_indexes, FoldIndexFun, Acc, Opts, State,
		fun indexes_filter/5).

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
	any(),
	[{atom(), term()}],
	state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_keys(FoldKeysFun, Acc, Opts, State) ->
	case proplists:get_value(bucket, Opts) of
		undefined ->
			fold_all(fold_keys, FoldKeysFun, Acc, Opts, State,
				fun default_backend_filter/5);
		Bucket ->
			fold_in_bucket(Bucket, fold_keys, FoldKeysFun, Acc, Opts, State)
	end.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
	any(),
	[{atom(), term()}],
	state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_objects(FoldObjectsFun, Acc, Opts, State) ->
	case proplists:get_value(bucket, Opts) of
		undefined ->
			fold_all(fold_objects, FoldObjectsFun, Acc, Opts, State,
				fun default_backend_filter/5);
		Bucket ->
			fold_in_bucket(Bucket, fold_objects, FoldObjectsFun, Acc, Opts, State)
	end.

%% @doc Delete all objects from the different backends
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{backends=Backends}=State) ->
	Fun = fun({Name, Module, SubState}) ->
		case Module:drop(SubState) of
			{ok, NewSubState} ->
				{Name, Module, NewSubState};
			{error, Reason, NewSubState} ->
				{error, Reason, NewSubState}
		end
		  end,
	DropResults = [Fun(Backend) || Backend <- Backends],
	{Errors, UpdBackends} =
		lists:splitwith(fun error_filter/1, DropResults),
	case Errors of
		[] ->
			{ok, State#state{backends=UpdBackends}};
		_ ->
			{error, Errors, State#state{backends=UpdBackends}}
	end.

drop_backend(Name, #state{backends=Backends} = State) ->
	lager:info("Attempting to drop backend name: ~p and list: ~p~n", [Name, Backends]),
	case lists:keyfind(Name, 1, Backends) of
		false ->
			error;
		{Name, Module, ModState} ->
			case Module:drop(ModState) of
				{ok, NewSubState} ->
					Backend = {Name, Module, NewSubState},
					lager:info("Dropping backend Name: ~p, Return from Bitcask backend with the new state: ~p~n", [Name, Backend]),
					NewBackends = lists:keydelete(Name, 1, Backends),
					lager:info("Backends before deleting: ~p~n", [Backends]),
					lager:info("Backends after deleting: ~p~n", [NewBackends]),
					ok = clean_data_dir(NewSubState),
					{ok, State#state{backends=NewBackends}};
				{error, Reason, NewSubState} ->
					{error, Reason, NewSubState}
			end
	end.

clean_data_dir(SubState) ->
	DataRoot = element(6, SubState),
	case file:list_dir(DataRoot) of
		{ok, []} ->
			file:del_dir(DataRoot);
		_ ->
			ok
	end.


%% @doc Returns true if the backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(#state{backends=Backends}) ->
	Fun = fun({_, Module, SubState}) ->
		Module:is_empty(SubState)
		  end,
	lists:all(Fun, Backends).

%% @doc Not currently supporting data size
%% @todo Come up with a way to reflect mixed backend data sizes,
%% as level reports in bytes, bitcask in # of keys.
-spec data_size(state()) -> undefined.
data_size(_) ->
	undefined.

%% @doc Get the status information for this backend
-spec status(state()) -> [{atom(), term()}].
status(#state{backends=Backends}) ->
	%% @TODO Reexamine how this is handled
	%% all backend mods return a proplist from Mod:status/1
	%% So as to tag the backend with its mod, without
	%% breaking this API list of two tuples return,
	%% add the tuple {mod, Mod} to the status for each
	%% backend.
	[{N, [{mod, Mod} | Mod:status(ModState)]} || {N, Mod, ModState} <- Backends].

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(Ref, Msg, #state{backends=Backends}=State) ->
	%% Pass the callback on to all submodules - their responsbility to
	%% filter out if they need it.
	_ = [Mod:callback(Ref, Msg, ModState) || {_N, Mod, ModState} <- Backends],
	{ok, State}.

set_legacy_indexes(State=#state{backends=Backends}, WriteLegacy) ->
	NewBackends = [{I, Mod, maybe_set_legacy_indexes(Mod, ModState, WriteLegacy)} ||
		{I, Mod, ModState} <- Backends],
	State#state{backends=NewBackends}.

maybe_set_legacy_indexes(Mod, ModState, WriteLegacy) ->
	case backend_can_index_reformat(Mod, ModState) of
		true -> Mod:set_legacy_indexes(ModState, WriteLegacy);
		false -> ModState
	end.

mark_indexes_fixed(State=#state{backends=Backends}, ForUpgrade) ->
	NewBackends = mark_indexes_fixed(Backends, [], ForUpgrade),
	{ok, State#state{backends=NewBackends}}.

mark_indexes_fixed([], NewBackends, _) ->
	lists:reverse(NewBackends);
mark_indexes_fixed([{I, Mod, ModState} | Backends], NewBackends, ForUpgrade) ->
	Res = maybe_mark_indexes_fixed(Mod, ModState, ForUpgrade),
	case Res of
		{error, Reason} ->
			{error, Reason};
		{ok, NewModState} ->
			mark_indexes_fixed(Backends, [{I, Mod, NewModState} | NewBackends], ForUpgrade)
	end.

maybe_mark_indexes_fixed(Mod, ModState, ForUpgrade) ->
	case backend_can_index_reformat(Mod, ModState) of
		true -> Mod:mark_indexes_fixed(ModState, ForUpgrade);
		false -> {ok, ModState}
	end.

fix_index(BKeys, ForUpgrade, State) ->
	% Group keys per bucket
	PerBucket = lists:foldl(fun(BK={B,_},D) -> dict:append(B,BK,D) end, dict:new(), BKeys),
	Result =
		dict:fold(
			fun(Bucket, StorageKey, Acc = {Success, Ignore, Errors}) ->
				{_, Mod,  ModState} = Backend = get_backend(Bucket, State),
				case backend_can_index_reformat(Mod, ModState) of
					true ->
						{S, I, E} = backend_fix_index(Backend, Bucket,
							StorageKey, ForUpgrade),
						{Success + S, Ignore + I, Errors + E};
					false ->
						Acc
				end
			end, {0, 0, 0}, PerBucket),
	{reply, Result, State}.

backend_fix_index({_, Mod, ModState}, Bucket, StorageKey, ForUpgrade) ->
	case Mod:fix_index(StorageKey, ForUpgrade, ModState) of
		{reply, Reply, _UpModState} ->
			Reply;
		{error, Reason} ->
			lager:error("Failed to fix index for bucket ~p, key ~p, backend ~p: ~p",
				[Bucket, StorageKey, Mod, Reason]),
			{0, 0, length(StorageKey)}
	end.

-spec fixed_index_status(state()) -> boolean().
fixed_index_status(#state{backends=Backends}) ->
	lists:foldl(fun({_N, Mod, ModState}, Acc) ->
		Status = Mod:status(ModState),
		case fixed_index_status(Mod, ModState, Status) of
			undefined -> Acc;
			Res ->
				case Acc of
					undefined -> Res;
					_ -> Res andalso Acc
				end
		end
				end,
		undefined,
		Backends).

fixed_index_status(Mod, ModState, Status) ->
	case backend_can_index_reformat(Mod, ModState) of
		true -> proplists:get_value(fixed_indexes, Status);
		false -> undefined
	end.



transfer_to_splits(CurrentBitcaskDir, Splits) ->
	lager:info("Top level bitcask dir fed into function call: ~p~n", [CurrentBitcaskDir]),
	lists:foreach(
		fun(BitcaskDir) ->
			case bitcask:open(BitcaskDir, []) of
				Ref when is_reference(Ref) ->
					lists:foreach(fun(Bucket) -> fold_data(Ref, Bucket) end, Splits);
				Reason ->
					lager:error("Failed to open bitcask dir: ~p for reason: ~p~n", [BitcaskDir, Reason])
			end
		end, filelib:wildcard(CurrentBitcaskDir ++ "*")).


fold_data(Ref, Bucket) ->
	lager:info("Test get request: ~p~n", [bitcask:get(Ref, make_kd(2, Bucket, <<"key-8">>))]),
	Acc1 = [],
	FoldKeysFun = fun(Bucket1, Key, Acc) -> [{Bucket1, Key} | Acc] end,
	FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
	{FoldResult, _Bucketset} =
		bitcask:fold_keys(Ref, FoldFun, {Acc1, sets:new()}),
	lager:info("Folding data, currently read: ~p~n", [FoldResult]),
	case FoldResult of
		{error, _} ->
			FoldResult;
		_ ->
			{ok, FoldResult}
	end.

fold_keys_fun(FoldKeysFun, undefined) ->
	fun(#bitcask_entry{key=BK}, Acc) ->
		{Bucket, Key} = bk_to_tuple(BK),
		FoldKeysFun(Bucket, Key, Acc)
	end;
fold_keys_fun(FoldKeysFun, Bucket) ->
	fun(#bitcask_entry{key=BK}, Acc) ->
		{B, Key} = bk_to_tuple(BK),
		case B =:= Bucket of
			true ->
				FoldKeysFun(B, Key, Acc);
			false ->
				Acc
		end
	end.

bk_to_tuple(<<?VERSION_2:7, HasType:1, Sz:16/integer,
	TypeOrBucket:Sz/bytes, Rest/binary>>) ->
	case HasType of
		0 ->
			%% no type, first field is bucket
			{TypeOrBucket, Rest};
		1 ->
			%% has a tyoe, extract bucket as well
			<<BucketSz:16/integer, Bucket:BucketSz/bytes, Key/binary>> = Rest,
			{{TypeOrBucket, Bucket}, Key}
	end;
bk_to_tuple(<<?VERSION_1:7, HasType:1, Sz:16/integer,
	TypeOrBucket:Sz/bytes, Rest/binary>>) ->
	case HasType of
		0 ->
			%% no type, first field is bucket
			{TypeOrBucket, Rest};
		1 ->
			%% has a tyoe, extract bucket as well
			<<BucketSz:16/integer, Bucket:BucketSz/bytes, Key/binary>> = Rest,
			{{TypeOrBucket, Bucket}, Key}
	end;
bk_to_tuple(<<131:8,_Rest/binary>> = BK) ->
	binary_to_term(BK).

make_kd(0, Bucket, Key) ->
	term_to_binary({Bucket, Key});
make_kd(1, {Type, Bucket}, Key) ->
	TypeSz = size(Type),
	BucketSz = size(Bucket),
	<<?VERSION_1:7, 1:1, TypeSz:16/integer, Type/binary,
		BucketSz:16/integer, Bucket/binary, Key/binary>>;
make_kd(1, Bucket, Key) ->
	BucketSz = size(Bucket),
	<<?VERSION_1:7, 0:1, BucketSz:16/integer,
		Bucket/binary, Key/binary>>;
make_kd(2, {Type, Bucket}, Key) ->
	TypeSz = size(Type),
	BucketSz = size(Bucket),
	<<?VERSION_BYTE:7, 1:1, TypeSz:16/integer, Type/binary, BucketSz:16/integer, Bucket/binary, Key/binary>>;
make_kd(2, Bucket, Key) ->
	BucketSz = size(Bucket),
	<<?VERSION_BYTE:7, 0:1, BucketSz:16/integer, Bucket/binary, Key/binary>>.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
%% Given a Bucket name and the State, return the
%% backend definition. (ie: {Name, Module, SubState})
%% If no split backend exists, use the default.
get_backend(Bucket, State = #state{backends=Backends, default_backend=DefaultBackend}) ->
	%% Ensure that a backend by that name exists...
	case lists:keyfind(Bucket, 1, Backends) of
		false ->
			case riak_core_metadata:get({split_backend, all}, use_default_backend, [{default, true}]) of
				true ->
					lists:keyfind(DefaultBackend, 1, Backends);    %% if no backend, return default
				_ ->
					{error, State}
			end;
		Backend -> Backend
	end.

%% @private
%% @doc Update the state for one of the
%% composing backends of this multi backend.
update_backend_state(Backend,
	Module,
	ModState,
	State=#state{backends=Backends}) ->
	NewBackends = lists:keyreplace(Backend,
		1,
		Backends,
		{Backend, Module, ModState}),
	State#state{backends=NewBackends}.

%% @private
%% @doc Shared code used by all the backend fold functions.
fold_all(ModFun, FoldFun, Acc, Opts, State, BackendFilter) ->
	Backends = State#state.backends,
	try
		AsyncFold = lists:member(async_fold, Opts),
		{Acc0, AsyncWorkList} =
			lists:foldl(backend_fold_fun(ModFun,
				FoldFun,
				Opts,
				AsyncFold,
				BackendFilter),
				{Acc, []},
				Backends),

		%% We have now accumulated the results for all of the
		%% synchronous backends. The next step is to wrap the
		%% asynchronous work in a function that passes the accumulator
		%% to each successive piece of asynchronous work.
		case AsyncWorkList of
			[] ->
				%% Just return the synchronous results
				{ok, Acc0};
			_ ->
				AsyncWork =
					fun() ->
						lists:foldl(async_fold_fun(), Acc0, AsyncWorkList)
					end,
				{async, AsyncWork}
		end
	catch
		Error ->
			Error
	end.

fold_in_bucket(Bucket, ModFun, FoldFun, Acc, Opts, State) ->
	case get_backend(Bucket, State) of
		{error, State} ->
			{ok, Acc};
		{_Name, Module, SubState} ->
			Module:ModFun(FoldFun,
				Acc,
				Opts,
				SubState)
	end.

default_backend_filter(Opts, Name, _Module, _SubState, ModCaps) ->
	filter_on_backend_opts(Opts, Name) andalso
		filter_on_index_reformat(Opts, ModCaps).

%% @doc Skips folding if index reformat operation on
%% non-index reformat capable backend.
filter_on_index_reformat(Opts, ModCaps) ->
	Indexes = lists:keyfind(index, 1, Opts),
	CanReformatIndex = lists:member(index_reformat, ModCaps),
	case {Indexes, CanReformatIndex} of
		{{index, incorrect_format, _ForUpgrade}, false} ->
			false;
		_ ->
			true
	end.

%% @doc If a {backend, [BACKEND_LIST]} option is specified,
%% don't fold over any backends that aren't in the list.
filter_on_backend_opts(Opts, Name) ->
	case lists:keyfind(backend, 1, Opts) of
		false ->
			true; % If no {backend, [...]} option is set, don't filter anything here
		{backend, BackendList} ->
			lists:member(Name, BackendList)
	end.

%% @doc Skips folding on backends that do not support indexes.
indexes_filter(_Opts, _Name, _Module, _SubState, ModCaps) ->
	lists:member(indexes, ModCaps).

%% @private
backend_fold_fun(ModFun, FoldFun, Opts, AsyncFold, BackendFilter) ->
	fun({Name, Module, SubState}, {Acc, WorkList}) ->
		%% Get the backend capabilities to determine
		%% if it supports asynchronous folding.
		{ok, ModCaps} = Module:capabilities(SubState),
		DoAsync = AsyncFold andalso lists:member(async_fold, ModCaps),
		case BackendFilter(Opts, Name, Module, SubState, ModCaps) of
			false ->
				{Acc, WorkList};
			true ->
				backend_fold_fun(Module,
					ModFun,
					SubState,
					FoldFun,
					Opts,
					{Acc, WorkList},
					DoAsync)
		end
	end.

backend_fold_fun(Module, ModFun, SubState, FoldFun, Opts, {Acc, WorkList}, true) ->
	AsyncWork =
		fun(Acc1) ->
			Module:ModFun(FoldFun,
				Acc1,
				Opts,
				SubState)
		end,
	{Acc, [AsyncWork | WorkList]};
backend_fold_fun(Module, ModFun, SubState, FoldFun, Opts, {Acc, WorkList}, false) ->
	Result = Module:ModFun(FoldFun,
		Acc,
		Opts,
		SubState),
	case Result of
		{ok, Acc1} ->
			{Acc1, WorkList};
		{error, Reason} ->
			throw({error, {Module, Reason}})
	end.

async_fold_fun() ->
	fun(AsyncWork, Acc) ->
		case AsyncWork(Acc) of
			{ok, Acc1} ->
				Acc1;
			{async, AsyncFun} ->
				AsyncFun();
			{error, Reason} ->
				throw({error, Reason})
		end
	end.

%% @private
%% @doc Function to filter error results when
%% calling a function on the entire list of
%% backends composing this multi backend.
error_filter({error, _, _}) ->
	true;
error_filter(_) ->
	false.

backend_can_index_reformat(Mod, ModState) ->
	{ok, Caps} = Mod:capabilities(ModState),
	lists:member(index_reformat, Caps).

metadata_config_puts(Name, Mod) ->
	case Mod of
		riak_kv_bitcask_backend ->
			riak_core_metadata:put({split_backend, bitcask}, default, Name);
		riak_kv_eleveldb_backend ->
			riak_core_metadata:put({split_backend, leveldb}, default, Name);
		riak_kv_memory_backend ->
			riak_core_metadata:put({split_backend, memory}, default, Name)
	end.

%%	BitcaskConf = app_helper:get_env(bitcask),
%%	LevelDBConf = app_helper:get_env(eleveldb),
%%	MemoryConf  = app_helper:get_env(memory),

%%	riak_core_metadata:put({split_backend, all}, use_default_backend, true),
%%	riak_core_metadata:put({split_backend, bitcask}, default, {riak_kv_bitcask_backend, Name}),
%%	riak_core_metadata:put({split_backend, leveldb}, default, {riak_kv_eleveldb_backend, Name}),
%%	riak_core_metadata:put({split_backend, memory},  default, {riak_kv_memory_backend, MemoryConf}).
%% TODO: Initial start up config needs adding to metadata, but doing it atr this stage triggers a race condition due to riak_kv_vnode not having finished starting up.
%% TODO: Need to figure out how to push to the metadata without propogating to the vnode event.
%%	lists:foreach(fun
%%					  ({Name, riak_kv_bitcask_backend, ModConf}) ->
%%						  riak_core_metadata:put({split_backend, bitcask}, binary_to_atom(Name, latin1), {Name, riak_kv_bitcask_backend, ModConf});
%%					  ({Name, riak_kv_eleveldb_backend, ModConf}) ->
%%						  riak_core_metadata:put({split_backend, leveldb}, binary_to_atom(Name, latin1), {Name, riak_kv_eleveldb_backend, ModConf});
%%					  ({Name, riak_kv_memory_backend, ModConf}) ->
%%						  riak_core_metadata:put({split_backend, memory}, binary_to_atom(Name, latin1), {Name, riak_kv_memory_backend, ModConf})
%%				  end, Defs).

%%fetch_metadata_backends(Defs) ->
%%	Itr  = riak_core_metadata:get({split_backend, bitcask}, splits, [{default, []}]),
%%	Itr1 = riak_core_metadata:get({split_backend, leveldb}, splits, [{default, []}]),
%%	Itr2 = riak_core_metadata:get({split_backend, memory}, splits, [{default, []}]),
%%	Itrs = [{memory, Itr2}, {leveldb, Itr1}, {bitcask, Itr}],
%%
%%	Backends = lists:foldl(fun({Type, Splits}, Acc) -> iterate2(Type, Splits, Acc, Defs) end, [], Itrs),
%%	lists:flatten([Backends | Defs]).

fetch_metadata_backends(Defs) ->
	Itr = riak_core_metadata:iterator({split_backend, bitcask}),
	Itr1 = riak_core_metadata:iterator({split_backend, leveldb}),
	Itr2 = riak_core_metadata:iterator({split_backend, memory}),
	Itrs = [{memory, Itr2}, {leveldb, Itr1}, {bitcask, Itr}],

	NewDefs = lists:append(Defs, [{default}]),
	Backends1 = lists:foldl(fun({Type, Iterator}, Acc) -> iterate(Type, Iterator, Acc, NewDefs) end, [], Itrs),
	lists:flatten([Backends1 | Defs]).
%%	lists:flatten([X || X <- Backends1, X =/= ['$deleted']]).

iterate(Type, Itr, Acc, Defs) ->
	case riak_core_metadata:itr_done(Itr) of
		true ->
			Acc;
		false ->
			{Key, Val} = riak_core_metadata:itr_key_values(Itr),
			NewItr = riak_core_metadata:itr_next(Itr),
			case lists:keymember(Key, 1, Defs) of
				true ->
					iterate(Type, NewItr, Acc, Defs);
				false ->
					case Val of
						['$deleted'] ->
							iterate(Type, NewItr, Acc, Defs);
						[[]] ->
							Config = riak_kv_util:get_backend_config(Key, Type),
							iterate(Type, NewItr, [Config | Acc], Defs)
					end
			end
	end.

%%iterate2(_Type, [], Acc, _Defs) ->
%%	Acc;
%%iterate2(Type, [Name | Splits], Acc, Defs) ->
%%	case lists:keymember(Name, 1, Defs) of
%%		false ->
%%			Config = riak_kv_util:get_backend_config(Name, Type),
%%			iterate2(Type, Splits, [Config | Acc], Defs);
%%		true ->
%%			iterate2(Type, Splits, Acc, Defs)
%%	end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

split_backend_test_() ->
	{foreach,
		fun() ->
			crypto:start(),

			%% start the ring manager
			{ok, P1} = riak_core_ring_events:start_link(),
			{ok, P2} = riak_core_ring_manager:start_link(test),
			startup_metadata_apps(),
			application:load(riak_core),
			application:set_env(riak_core, default_bucket_props, []),

			%% Have to do some prep for bitcask
			application:load(bitcask),
			?assertCmd("rm -rf test/bitcask-backend"),
			application:set_env(bitcask, data_root, "test/bitcask-backend"),

			[P1, P2]
		end,
		fun([P1, P2]) ->
			crypto:stop(),
			?assertCmd("rm -rf test/bitcask-backend"),
			unlink(P1),
			unlink(P2),
			stop_metadata_apps(),
			catch exit(P1, kill),
			catch exit(P2, kill),
			wait_until_dead(P1),
			wait_until_dead(P2)
		end,
		[
			fun(_) ->
				{"simple test",
					fun() ->
						%% Run the standard backend test...
						Config = sample_config(),
						backend_test_util:standard_test_fun(?MODULE, Config)
					end
				}
			end,
			fun(_) ->
				{"check_existing_backend_test",
					fun() ->
						B1 = <<"b1">>,
						B2 = <<"b2">>,
						Config = sample_split_bucket_config([B1, B2], riak_kv_bitcask_backend),
						{ok, State} = start(42, Config),

						true = check_backend_exists(B1, State),
						true = check_backend_exists(B2, State),

						false = check_backend_exists(should_not_exist, State)
					end
				}
			end,
			fun(_) ->
				{"start_additional_backends_test",
					fun() ->
						B1 = <<"b1">>,
						B2 = <<"b2">>,
						Config = sample_split_bucket_config([B1, B2], riak_kv_bitcask_backend),
						{ok, State} = start(42, Config),

						true = check_backend_exists(B1, State),
						true = check_backend_exists(B2, State),

						false = check_backend_exists(new_backend, State),

						NewConf = [{new_backend, riak_kv_bitcask_backend, []}, {new_backend2, riak_kv_eleveldb_backend, []}],

						{ok, NewState} = start_additional_backends(42, NewConf, State),

						true = check_backend_exists(new_backend, NewState),
						true = check_backend_exists(new_backend2, NewState)
					end
				}
			end,
			fun(_) ->
				{"get_backend_test",
					fun() ->
						%% Start the backend...
						B1 = <<"b1">>,
						B2 = <<"b2">>,
						Config = sample_split_bucket_config([B1, B2], riak_kv_bitcask_backend),
						{ok, State} = start(42, Config),
						%% Check our buckets...
						{B1, riak_kv_bitcask_backend, _} = get_backend(B1, State),
						{B2, riak_kv_bitcask_backend, _} = get_backend(B2, State),

						%% Check the default...
						{B1, riak_kv_bitcask_backend, _} = get_backend(<<"b3">>, State),

						riak_core_metadata:put({split_backend, all}, use_default_backend, false),

						%% Check that error is thrown when flag set to not use default
						?assertThrow({error, "Put rejected due to no backend being configured", State}, get_backend(<<"b3">>, State))
					end
				}
			end,
			fun(_) ->
				{"fold_buckets test",
					fun() ->
						B1 = <<"first_backend">>, B2 = <<"second_backend">>,
						K1 = <<"k1">>, K2 = <<"k2">>,
						V1 = <<"v1">>, V2 = <<"v2">>,
						Config = sample_split_bucket_config([B1, B2], riak_kv_bitcask_backend),

						%% Start the backend...
						{ok, State} = start(40, Config),

						%% Create some placeholder values in the buckets so that they show
						%% up when we run the fold operation...
						put(B1, K1, [], V1, State),
						put(B2, K2, [], V2, State),

						%% First do a basic fold with no options to make sure we iterate
						%% correctly over all buckets:
						FoldFun = fun(Bucket, Acc) -> [Bucket | Acc] end,
						{ok, FoldRes} = fold_buckets(FoldFun, [], [], State),
						[B1, B2] = lists:sort(FoldRes),

						%% Then filter on a specific backend, and we should only see
						%% the bucket using that backend:
						{ok, [B2]} = fold_buckets(FoldFun, [], [{backend, [B2]}], State)
					end
				}
			end,
			fun(_) ->
				{"fold_keys test",
					fun() ->
						B1 = <<"b1">>, B2 = <<"b2">>,
						K1 = <<"k1">>, K2 = <<"k2">>,
						V1 = <<"v1">>, V2 = <<"v2">>,

						%% Setup config for backends split by bucket adn start them
						Config = sample_split_bucket_config([B1, B2], riak_kv_bitcask_backend),
						{ok, State} = start(42, Config),

						put(B1, K1, [], V1, State),
						put(B2, K2, [], V2, State),

						FoldFun = fun(_Bucket, Key, Acc) -> [Key | Acc] end,

						{ok, _FoldRes} = fold_keys(FoldFun, [], [], State)
					end
				}
			end,
			fun(_) ->
				{"start error with invalid backend test",
					fun() ->
						%% Attempt to start the backend with a
						%% nonexistent backend specified
						?assertEqual({error, [{riak_kv_devnull_backend, undef}]},
							start(42, bad_backend_config()))
					end
				}
			end
		]
	}.

-ifdef(EQC).

eqc_test_() ->
	{spawn,
		[{inorder,
			[{setup,
				fun setup/0,
				fun cleanup/1,
				[{timeout, 60000, [?_assertEqual(true,
					backend_eqc:test(?MODULE, true, sample_config()))]},
					{timeout, 60000, [?_assertEqual(true,
						backend_eqc:test(?MODULE, true, async_fold_config()))]}
				]}]}]}.

setup() ->
	%% Start the ring manager...
	crypto:start(),
	{ok, P1} = riak_core_ring_events:start_link(),
	{ok, P2} = riak_core_ring_manager:start_link(test),

	%% Set some buckets...
	application:load(riak_core), % make sure default_bucket_props is set
	application:set_env(riak_core, default_bucket_props, []),
	riak_core_bucket:set_bucket(<<"b1">>, [{backend, first_backend}]),
	riak_core_bucket:set_bucket(<<"b2">>, [{backend, second_backend}]),

	{P1, P2}.

cleanup({P1, P2}) ->
	crypto:stop(),
	application:stop(riak_core),

	unlink(P1),
	unlink(P2),
	catch exit(P1, kill),
	catch exit(P2, kill),
	wait_until_dead(P1),
	wait_until_dead(P2).

async_fold_config() ->
	[
		{storage_backend, riak_kv_split_backend},
		{split_backend_default, second_backend},
		{split_backend, [
			{first_backend, riak_kv_memory_backend, []},
			{second_backend, riak_kv_memory_backend, []}
		]}
	].

-endif. % EQC

%% Check extra callback messages are ignored by backends
extra_callback_test() ->
	%% Have to do some prep for bitcask
	application:load(bitcask),
	?assertCmd("rm -rf test/bitcask-backend"),
	application:set_env(bitcask, data_root, "test/bitcask-backend"),

	%% Have to do some prep for eleveldb
	application:load(eleveldb),
	?assertCmd("rm -rf test/eleveldb-backend"),
	application:set_env(eleveldb, data_root, "test/eleveldb-backend"),

	startup_metadata_apps(),


	%% Start up multi backend
	Config = [{storage_backend, riak_kv_split_backend},
		{split_backend_default, memory},
		{split_backend,
			[{bitcask, riak_kv_bitcask_backend, []},
				{memory, riak_kv_memory_backend, []},
				{eleveldb, riak_kv_eleveldb_backend, []}]}],
	{ok, State} = start(0, Config),
	callback(make_ref(), ignore_me, State),
	stop(State),
	stop_metadata_apps(),
	application:stop(bitcask).

bad_config_test() ->
	application:unset_env(riak_kv, split_backend),
	ErrorReason = split_backend_config_unset,
	?assertEqual({error, ErrorReason}, start(0, [])).

sample_config() ->
	[
		{storage_backend, riak_kv_split_backend},
		{split_backend_default, second_backend},
		{split_backend, [
			{first_backend, riak_kv_memory_backend, []},
			{second_backend, riak_kv_memory_backend, []}
		]}
	].

sample_split_bucket_config(Buckets, Type) ->
	Backends = [{Bucket, Type, [{data_root, "test/bitcask-backend/" ++ binary_to_list(Bucket)}]} || Bucket <- Buckets],
	[
		{storage_backend, riak_kv_split_backend},
		{split_backend_default, hd(Buckets)},
		{split_backend, Backends}
	].

bad_backend_config() ->
	[
		{storage_backend, riak_kv_split_backend},
		{split_backend_default, second_backend},
		{split_backend, [
			{first_backend, riak_kv_devnull_backend, []},
			{second_backend, riak_kv_memory_backend, []}
		]}
	].

%% Minor sin of cut-and-paste....
wait_until_dead(Pid) when is_pid(Pid) ->
	Ref = monitor(process, Pid),
	receive
		{'DOWN', Ref, process, _Obj, Info} ->
			Info
	after 10*1000 ->
		exit({timeout_waiting_for, Pid})
	end;
wait_until_dead(_) ->
	ok.

startup_metadata_apps() ->
	riak_core_metadata_events:start_link(),
	riak_core_metadata_manager:start_link([{data_dir, "kv_split_backend_test_meta"}]),
	riak_core_metadata_hashtree:start_link().

stop_metadata_apps() ->
	riak_kv_test_util:stop_process(riak_core_metadata_events),
	riak_kv_test_util:stop_process(riak_core_metadata_manager),
	riak_kv_test_util:stop_process(riak_core_metadata_hashtree).

-endif.
