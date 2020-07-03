%% -------------------------------------------------------------------
%%
%% riak_kv_bitcask_backend: Bitcask Driver for Riak
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

-module(riak_kv_bitcask_backend).
-behavior(riak_kv_backend).

%% KV Backend API
-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2,
         start_additional_split/2,
         stop/1,
         get/3,
         put/5,
         put/6,
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         check_backend_exists/2,
         activate_backend/2,
         deactivate_backend/2,
         remove_backend/2,
         is_backend_active/2,
         has_merged/2,
         fetch_metadata_backends/1,
         special_merge/3,
         reverse_merge/3,
         callback/3]).

-export([data_size/1,
         encode_disk_key/2,
         decode_disk_key/1]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([prop_bitcask_backend/0]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("bitcask/include/bitcask.hrl").

-define(MERGE_CHECK_INTERVAL, timer:minutes(3)).
-define(MERGE_CHECK_JITTER, 0.3).
-define(UPGRADE_CHECK_INTERVAL, timer:seconds(10)).
-define(UPGRADE_MERGE_BATCH_SIZE, 5).
-define(UPGRADE_FILE, "upgrade.txt").
-define(MERGE_FILE, "merge.txt").
-define(VERSION_FILE, "version.txt").
-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold,size,backend_reap]).
-define(TERMINAL_POSIX_ERRORS, [eacces, erofs, enodev, enospc]).

%% must not be 131, otherwise will match t2b in error
%% yes, I know that this is horrible.
-define(VERSION_0, 131).
-define(VERSION_1, 1).
-define(VERSION_2, 2).
-define(VERSION_3, 3).

-define(CURRENT_VERSION, ?VERSION_3).
-define(ENCODE_DISK_KEY_FUN,        fun encode_disk_key/2).
-define(DECODE_DISK_KEY_FUN,        fun decode_disk_key/1).
-define(ENCODE_BITCASK_KEY,         fun make_bitcask_key/3).
-define(DECODE_BITCASK_KEY,         fun make_riak_key/1).
-define(FIND_SPLIT_FUN,             fun find_split/2).
-define(CHECK_AND_UPGRADE_KEY_FUN,  fun check_and_upgrade_key/2).

-record(state, {ref :: reference() | undefined,
                data_dir :: string(),
                opts :: [{atom(), term()}],
                partition :: integer(),
                root :: string(),
                key_vsn :: integer()}).

-type state() :: #state{}.
-type config() :: [{atom(), term()}].
-type version() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}.

%%
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
capabilities(_) ->
    {ok, ?CAPABILITIES}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(_, _) ->
    {ok, ?CAPABILITIES}.

%% @doc Start the bitcask backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config0) ->
    rand:seed(exrop, os:timestamp()),
%%    Config0 = proplists:get_value(start_md_backends, Config01, true),
    BaseConfig = proplists:delete(small_keys, Config0),
    KeyOpts =
        [
            {encode_disk_key_fun,       ?ENCODE_DISK_KEY_FUN},
            {decode_disk_key_fun,       ?DECODE_DISK_KEY_FUN},
            {encode_riak_key,           ?ENCODE_BITCASK_KEY},
            {decode_riak_key,           ?DECODE_BITCASK_KEY},
            {find_split_fun,            ?FIND_SPLIT_FUN},
            {check_and_upgrade_key_fun, ?CHECK_AND_UPGRADE_KEY_FUN}
        ],
    Config = BaseConfig ++ KeyOpts,
    KeyVsn = ?CURRENT_VERSION,

    %% Get the data root directory
    case app_helper:get_prop_or_env(data_root, Config, bitcask) of
        undefined ->
            lager:error("Failed to create bitcask dir: data_root is not set"),
            {error, data_root_unset};
        DataRoot ->
            %% Check if a directory exists for the partition
            PartitionStr = integer_to_list(Partition),
            case get_data_dir(DataRoot, PartitionStr) of
                {ok, DataDir} ->

                    BitcaskDir = filename:join(DataRoot, DataDir),
                    UpgradeRet = maybe_start_upgrade(BitcaskDir),
                    BitcaskOpts = set_mode(read_write, Config),
                    BitcaskOpts1 = [{upgrade_key, true} | BitcaskOpts],
                    Backends =
                        case proplists:get_value(start_md_backends, Config, true) of
                            true ->
                                fetch_metadata_backends(Partition);
                            false ->
                                []
                        end,
                    case Backends of
                        [] ->
                            start_split(BitcaskDir, BitcaskOpts, UpgradeRet, DataDir, DataRoot, BitcaskOpts1, Partition, KeyVsn);
                        _ ->
                            {ok, State} = start_split(BitcaskDir, BitcaskOpts, UpgradeRet, DataDir, DataRoot, BitcaskOpts1, Partition, KeyVsn),
                            Return = start_additional_split(Backends, State),
                            Return
                    end;
                {error, Reason} ->
                    lager:error("Failed to start bitcask backend: ~p\n",
                                [Reason]),
                    {error, Reason}
            end
    end.

-spec start_split(string(), config(), no_upgrade | {upgrading, version()}, string(), string(), config(), integer(), integer()) ->
    {ok, state()} | {error, term()}.
start_split(BitcaskDir, BitcaskOpts, UpgradeRet, DataDir, DataRoot, BitcaskOpts1, Partition, KeyVsn) ->
    case bitcask_manager:open(BitcaskDir, BitcaskOpts) of
        Ref when is_reference(Ref) ->
            check_fcntl(),
            schedule_merge(Ref),
            maybe_schedule_sync(Ref),
            maybe_schedule_upgrade_check(Ref, UpgradeRet),
            {ok, #state{ref=Ref,
                data_dir=DataDir,
                root=DataRoot,
                opts=BitcaskOpts1,
                partition=Partition,
                key_vsn=KeyVsn}};
        {error, Reason1} ->
            lager:error("Failed to start bitcask backend: ~p~n",
                [Reason1]),
            {error, Reason1}
    end.

-spec start_additional_split([{atom(), atom()}], state()) ->
    {ok, state()} | {error, term()}.
start_additional_split(Splits, State) when is_tuple(Splits) ->
    start_additional_split([Splits], State);
start_additional_split([], State) ->
    {ok, State};
start_additional_split([{Split, ActiveStatus} | Rest], #state{ref = Ref, data_dir=DataDir, root=DataRoot, opts=BitcaskOpts} = State) ->
    BitcaskDir = filename:join(DataRoot, DataDir),
    NewActiveStatusOpts = case ActiveStatus of
                          active ->
                              [{is_active, true}];
                          special_merge ->
                              [{has_merged, true}, {is_active, true}];
                          false ->
                              [{is_active, false}]
                      end,
    NewOpts = lists:flatten([{split, Split}, NewActiveStatusOpts | BitcaskOpts]),
    NewRef = bitcask_manager:open(Ref, BitcaskDir, NewOpts),
    start_additional_split(Rest, State#state{ref = NewRef}).

%% @doc Stop the bitcask backend
-spec stop(state()) -> ok.
stop(#state{ref=Ref}) ->
    case Ref of
        undefined ->
            ok;
        _ ->
            bitcask_manager:close(Ref)
    end.

%% @doc Retrieve an object from the bitcask backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {error, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, State) ->
    get(Bucket, Key, State, []).
get(Bucket, Key, #state{ref=Ref, key_vsn=KVers, partition = Partition}=State, Opts) ->
    Split = get_split(Bucket, Key, Partition, get),
    BitcaskKey = make_bitcask_key(KVers, {Bucket, Split}, Key),
    case bitcask_manager:get(Ref, BitcaskKey, [{split, binary_to_atom(Split, latin1)} | Opts]) of
        {ok, Value} ->
            {ok, Value, State};
        not_found  ->
            {error, not_found, State};
        {error, bad_crc}  ->
            lager:warning("Unreadable object ~p/~p discarded",
                          [Bucket,Key]),
            {error, not_found, State};
        {error, nofile}  ->
            {error, not_found, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Insert an object into the bitcask backend.
%% NOTE: The bitcask backend does not currently support
%% secondary indexing and the_IndexSpecs parameter
%% is ignored.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), integer(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, PrimaryKey, _IndexSpecs, Val, TstampExpire, #state{ref=Ref, key_vsn=KeyVsn, partition = Partition}=State) ->
    Split = get_split(Bucket, PrimaryKey, Partition, put),
    BitcaskKey = make_bitcask_key(KeyVsn, {Bucket, Split}, PrimaryKey),
    Opts = [{?TSTAMP_EXPIRE_KEY, TstampExpire}, {split, binary_to_atom(Split, latin1)}],
    case bitcask_manager:put(Ref, BitcaskKey, Val, Opts) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, PrimaryKey, _IndexSpecs, Val, #state{ref=Ref, key_vsn=KeyVsn, partition = Partition}=State) ->
    Split = get_split(Bucket, PrimaryKey, Partition, put),
    BitcaskKey = make_bitcask_key(KeyVsn, {Bucket, Split}, PrimaryKey),
    Opts = [{split, binary_to_atom(Split, latin1)}],
    case bitcask_manager:put(Ref, BitcaskKey, Val, Opts) of
        ok ->
            {ok, State};
        {error, Reason} ->
            lager:warning("Backend put error ~p", [Reason]),
            % Should crash if the error is a permanent file system error
            false =
                is_tuple(Reason) and
                    lists:member(element(2, Reason), ?TERMINAL_POSIX_ERRORS),
            {error, Reason, State}
    end.

%% @doc Delete an object from the bitcask backend
%% NOTE: The bitcask backend does not currently support
%% secondary indexing and the_IndexSpecs parameter
%% is ignored.
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()}.
delete(Bucket, Key, _IndexSpecs, #state{ref=Ref, key_vsn=KeyVsn, partition = Partition}=State) ->
    Split = get_split(Bucket, Key, Partition, delete),
    BitcaskKey = make_bitcask_key(KeyVsn, {Bucket, Split}, Key),
    ok = bitcask_manager:delete(Ref, BitcaskKey, [{split, binary_to_atom(Split, latin1)}]),
    {ok, State}.

%% TODO Splits are per bucket for now. How do we decide which split we want to fold buckets for? Or do we want to check all splits (pass in {all_splits, true} option)?
%% @doc Fold over all the buckets.
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_buckets(FoldBucketsFun, Acc, Opts, #state{opts=BitcaskOpts,
                                               data_dir=DataFile,
                                               ref=Ref,
                                               root=DataRoot}) ->
    FoldFun = fold_buckets_fun(FoldBucketsFun),
    FoldOpts = build_bitcask_fold_opts(Opts),
    case lists:member(async_fold, Opts) of
        true ->
            ReadOpts = set_mode(read_only, BitcaskOpts),

            BucketFolder =
                fun() ->
                        case bitcask_manager:open(filename:join(DataRoot, DataFile), ReadOpts) of
                            Ref1 when is_reference(Ref1) ->
                                try
                                    {Acc1, _} =
                                        bitcask_manager:fold_keys(Ref1,
                                                          FoldFun,
                                                          {Acc, sets:new()}, FoldOpts),
                                        Acc1
                                after
                                    bitcask_manager:close(Ref1)
                                end;
                            {error, Reason} ->
                                {error, Reason}
                        end
                end,
            {async, BucketFolder};
        false ->
            {FoldResult, _Bucketset} =
                bitcask_manager:fold_keys(Ref, FoldFun, {Acc, sets:new()}, FoldOpts),
            case FoldResult of
                {error, _} ->
                    FoldResult;
                _ ->
                    {ok, FoldResult}
            end
    end.
%% TODO List keys nto retrieving previously put data when in "deactive" state
%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()} | {error, term()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{opts=BitcaskOpts,
                                         data_dir=DataFile,
                                         ref=Ref,
                                         root=DataRoot,
                                         partition = Partition}) ->
    Bucket =  proplists:get_value(bucket, Opts, <<"undefined">>),
    FoldOpts = build_bitcask_fold_opts(Opts),
    FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
    case lists:member(async_fold, Opts) of
        true ->
            ReadOpts = set_mode(read_only, BitcaskOpts),
            HasMerged = bitcask_manager:has_merged(Ref, binary_to_atom(Bucket, latin1)),
            IsActive = bitcask_manager:is_active(Ref, binary_to_atom(Bucket, latin1)),
            NewOpts = case {IsActive, HasMerged} of
                          {false, false} ->
                              FoldOpts;
                          _ ->
                              [{split, binary_to_atom(Bucket, latin1)} | FoldOpts]
                      end,
            KeyFolder =
                fun() ->
                    Ref1 = open_read_only_backends(Bucket, filename:join(DataRoot, DataFile), HasMerged, IsActive, ReadOpts, Partition),
                        case Ref1 of
                            Ref1 when is_reference(Ref1) ->
                                try
                                    bitcask_manager:fold_keys(Ref1, FoldFun, Acc, NewOpts)
                                after
                                    bitcask_manager:close(Ref1)
                                end;
                            {error, Reason} ->
                                {error, Reason}
                        end
                end,
            {async, KeyFolder};
        false ->
            NewOpts = add_split_opts(Bucket, FoldOpts, Partition),
            FoldResult = bitcask_manager:fold_keys(Ref, FoldFun, Acc, NewOpts),
            case FoldResult of
                {error, _} ->
                    FoldResult;
                _ ->
                    {ok, FoldResult}
            end
    end.

build_bitcask_fold_opts(Opts) ->
    case lists:member(ignore_deletes_with_expiry, Opts) of
        true ->
            [{ignore_tstamp_expire_keys, true}];
        false ->
            lists:delete(ignore_deletes_with_expiry, Opts)
    end.


%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{opts=BitcaskOpts,
                                               data_dir=DataFile,
                                               ref=Ref,
                                               root=DataRoot,
                                               partition = Partition}) ->
    Bucket =  proplists:get_value(bucket, Opts, <<"undefined">>),
    FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
    FoldOpts = build_bitcask_fold_opts(Opts),
    case lists:member(async_fold, Opts) of
        true ->
            ReadOpts = set_mode(read_only, BitcaskOpts),
            HasMerged = bitcask_manager:has_merged(Ref, binary_to_atom(Bucket, latin1)),
            IsActive = bitcask_manager:is_active(Ref, binary_to_atom(Bucket, latin1)),
            NewOpts = case {IsActive, HasMerged} of
                          {false, false} ->
                              Opts;
                          _ ->
                              [{split, binary_to_atom(Bucket, latin1)} | FoldOpts]
                      end,
            ObjectFolder =
                fun() ->
                    Ref1 = open_read_only_backends(Bucket, filename:join(DataRoot, DataFile), HasMerged, IsActive, ReadOpts, Partition),
                    case Ref1 of
                        Ref1 when is_reference(Ref1) ->
                            try
                                bitcask_manager:fold(Ref1, FoldFun, Acc, NewOpts)
                            after
                                bitcask_manager:close(Ref1)
                            end;
                        {error, Reason} ->
                            {error, Reason}
                    end
                end,
            {async, ObjectFolder};
        false ->
            FoldResult = bitcask_manager:fold(Ref, FoldFun, Acc, FoldOpts),
            case FoldResult of
                {error, _} ->
                    FoldResult;
                _ ->
                    {ok, FoldResult}
            end
    end.

-spec open_read_only_backends(riak_object:bucket(), string(), atom(), atom(), config(), integer()) -> reference().
open_read_only_backends(Bucket, Dir, HasMerged, IsActive, Opts, Partition) ->
    case IsActive of
        false ->
            case HasMerged of
                false ->
                    bitcask_manager:open(Dir, Opts);
                true ->
                    SplitOpts = add_split_opts(Bucket, Opts, Partition),
                    OpenOpts = [{has_merged, HasMerged}, {is_active, IsActive} | SplitOpts],
                    Ref1 = bitcask_manager:open(Dir, Opts),
                    bitcask_manager:open(Ref1, Dir, OpenOpts)
            end;
        true ->
            case HasMerged of
                true ->
                    SplitOpts = add_split_opts(Bucket, Opts, Partition),
                    OpenOpts = [{has_merged, HasMerged}, {is_active, IsActive} | SplitOpts],
                    bitcask_manager:open(Dir, OpenOpts);
                false ->
                    SplitOpts = add_split_opts(Bucket, Opts, Partition),
                    OpenOpts = [{has_merged, HasMerged}, {is_active, IsActive} | SplitOpts],
                    Ref1 = bitcask_manager:open(Dir, Opts),
                    bitcask_manager:open(Ref1, Dir, OpenOpts)
            end;
        eliminate ->
            bitcask_manager:open(Dir, Opts)
    end.

%% @doc Delete all objects from this bitcask backend
%% @TODO once bitcask has a more friendly drop function
%%  of its own, use that instead.
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{ref=Ref,
            partition=Partition,
            root=DataRoot}=State) ->
    %% Close the bitcask reference
    bitcask_manager:close(Ref),

    PartitionStr = integer_to_list(Partition),
    PartitionDir = filename:join([DataRoot, PartitionStr]),

    %% Make sure the data directory is now empty
    data_directory_cleanup(PartitionDir),
    {ok, State#state{ref = undefined}}.

%% @doc Returns true if this bitcasks backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(#state{ref=Ref}) ->
    %% Estimate if we are empty or not as determining for certain
    %% requires a fold over the keyspace that may block. The estimate may
    %% return false when this bitcask is actually empty, but it will never
    %% return true when the bitcask has data.
    bitcask_manager:is_empty_estimate(Ref).

%% @doc Get the status information for this bitcask backend
-spec status(state()) -> [{atom(), term()}].
status(#state{ref=Ref}) ->
    {KeyCount, Status} = bitcask_manager:status(Ref),
    [{key_count, KeyCount}, {status, Status}].

-spec check_backend_exists(atom(), state()) -> boolean().
check_backend_exists(Split, #state{ref = Ref}) ->
    bitcask_manager:check_backend_exists(Ref, Split).

-spec activate_backend(atom(), state()) -> {ok, state()}.
activate_backend(Split, #state{ref = Ref} = State) ->
    NewRef = bitcask_manager:activate_split(Ref, Split),
    {ok, State#state{ref = NewRef}}.

-spec deactivate_backend(atom(), state()) -> {ok, state()}.
deactivate_backend(Split, #state{ref = Ref} = State) ->
    NewRef = bitcask_manager:deactivate_split(Ref, Split),
    {ok, State#state{ref = NewRef}}.

-spec remove_backend(atom(), state()) -> {ok, state()}.
remove_backend(Split, #state{ref = Ref} = State) ->
    NewRef = bitcask_manager:close(Ref, Split),
    {ok, State#state{ref = NewRef}}.

-spec is_backend_active(atom(), state()) -> atom().
is_backend_active(Split, #state{ref = Ref}) ->
    bitcask_manager:is_active(Ref, Split).

-spec has_merged(atom(), state()) -> atom().
has_merged(Split, #state{ref = Ref}) ->
    bitcask_manager:has_merged(Ref, Split).

-spec fetch_metadata_backends(integer()) -> [{atom(), atom()}].
-ifdef(TEST).
fetch_metadata_backends(_Partition)->
    [].
-else.
fetch_metadata_backends(Partition)->
    Itr = riak_core_metadata:iterator({split_backend, splits}),
    iterate(Itr, [], Partition).

iterate(Itr, Acc, Partition) ->
    case riak_core_metadata:itr_done(Itr) of
        true ->
            Acc;
        false ->
            {{Key, Node}, Val} = riak_core_metadata:itr_key_values(Itr),
            NewItr = riak_core_metadata:itr_next(Itr),
            case node() of
                Node ->
                    case Val of
                        ['$deleted'] ->
                            iterate(NewItr, Acc, Partition);
                        [Val1] ->
                            case lists:keyfind(Partition, 1, Val1) of
                                {Partition, State} ->
                                    iterate(NewItr, [{binary_to_atom(Key, latin1), State} | Acc], Partition);
                                false ->
                                    iterate(NewItr, Acc, Partition)
                            end
                    end;
                _ ->
                    iterate(NewItr, Acc, Partition)
            end
    end.
-endif.

-spec special_merge(atom(), atom(), state()) -> ok.
special_merge(Split1, Split2, #state{opts = Opts, ref = Ref, partition = Partition} = State) ->
    case {check_backend_exists(Split1, State), check_backend_exists(Split2, State)} of
        {true, true} ->
            case {is_backend_active(Split1, State), is_backend_active(Split2, State)} of
                {true, true} ->
                    Opts1 = [{partition, Partition} | Opts],
                    bitcask_manager:special_merge(Ref, Split1, Split2, Opts1);
                _ ->
                    lager:error("One or more of the backends scheduled for special_merge are not active: ~p and ~p~n", [Split1, Split2])
            end;
        _ ->
            lager:error("One or more of the backends scheduled for special_merge do not exist: ~p and ~p~n", [Split1, Split2])
    end.

-spec reverse_merge(atom(), atom(), state()) -> ok.
reverse_merge(Split1, Split2, #state{opts = Opts, ref = Ref, partition = Partition} = State) ->
    case {check_backend_exists(Split1, State), check_backend_exists(Split2, State)} of
        {true, true} ->
            case {is_backend_active(Split1, State), is_backend_active(Split2, State)} of
                {false, true} ->
                    Opts1 = [{partition, Partition} | Opts],
                    bitcask_manager:reverse_merge(Ref, Split1, Split2, Opts1);
                States ->
                    lager:error("One of the backends is not in the correct active state for reverse merge, Expected: {false, true}, Actual: ~p for backends: ~p~n", [States, {Split1, Split2}])
            end;
        _ ->
            lager:error("One of the backends scheduled for reverse merge does not exist, backends: ~p~n", [Split1, Split2])
    end.

%% @doc Get the size of the bitcask backend (in number of keys)
-spec data_size(state()) -> undefined | {non_neg_integer(), objects}.
data_size(State) ->
    Status = status(State),
    case proplists:get_value(key_count, Status) of
        undefined -> undefined;
        KeyCount -> {KeyCount, objects}
    end.

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(Ref,
         {sync, SyncInterval},
         #state{ref=Ref}=State) when is_reference(Ref) ->
    bitcask_manager:sync(Ref),
    schedule_sync(Ref, SyncInterval),
    {ok, State};
callback(Ref,
         merge_check,
         #state{ref=Ref,
                data_dir=DataDir,
                opts=BitcaskOpts,
                root=DataRoot}=State) when is_reference(Ref) ->
    case lists:member(riak_kv, riak_core_node_watcher:services(node())) of
        true ->
            BitcaskRoot = filename:join(DataRoot, DataDir),
            merge_check(Ref, BitcaskRoot, BitcaskOpts);
        false ->
            lager:debug("Skipping merge check: KV service not yet up"),
            ok
    end,
    schedule_merge(Ref),
    {ok, State};
callback(Ref,
         upgrade_check,
         #state{ref=Ref,
                data_dir=DataDir,
                root=DataRoot}=State) when is_reference(Ref) ->
    BitcaskRoot = filename:join(DataRoot, DataDir),
    case check_upgrade(BitcaskRoot) of
        finished ->
            case finalize_upgrade(BitcaskRoot) of
                {ok, Vsn} ->
                    lager:info("Finished upgrading to Bitcask ~s in ~s",
                               [version_to_str(Vsn), BitcaskRoot]),
                    {ok, State};
                {error, EndErr} ->
                    lager:error("Finalizing backend upgrade : ~p", [EndErr]),
                    {ok, State}
            end;
        pending ->
            _ = schedule_upgrade_check(Ref),
            {ok, State};
        {error, Reason} ->
            lager:error("Aborting upgrade in ~s : ~p", [BitcaskRoot, Reason]),
            {ok, State}
    end;
%% Ignore callbacks for other backends so multi backend works
callback(_Ref, _Msg, State) ->
    {ok, State}.

%% ===================================================================
%% Key Encode and Decode Functions
%% ===================================================================

%% Make the bitcask keydir key, based on the version number we are using and the bucket, key from riak
make_bitcask_key(0, Bucket, Key) ->
    term_to_binary({Bucket, Key});

make_bitcask_key(1, {Type, Bucket}, Key) ->
    TypeSz = size(Type),
    BucketSz = size(Bucket),
    <<?VERSION_1:7, 1:1, TypeSz:16/integer, Type/binary, BucketSz:16/integer, Bucket/binary, Key/binary>>;

make_bitcask_key(1, Bucket, Key) ->
    BucketSz = size(Bucket),
    <<?VERSION_1:7, 0:1, BucketSz:16/integer, Bucket/binary, Key/binary>>;

make_bitcask_key(2, {Type, Bucket}, Key) ->
    TypeSz = size(Type),
    BucketSz = size(Bucket),
    <<?VERSION_2:7, 1:1, TypeSz:16/integer, Type/binary, BucketSz:16/integer, Bucket/binary, Key/binary>>;

make_bitcask_key(2, Bucket, Key) ->
    BucketSz = size(Bucket),
    <<?VERSION_2:7, 0:1, BucketSz:16/integer, Bucket/binary, Key/binary>>;

make_bitcask_key(3, {Type, Bucket, Split}, Key) ->
    TypeSz = size(Type),
    BucketSz = size(Bucket),
    SplitSz = size(Split),
    <<?VERSION_3:7, 1:1, SplitSz:16/integer, Split/binary, TypeSz:16/integer, Type/binary, BucketSz:16/integer, Bucket/binary, Key/binary>>;

make_bitcask_key(3, {Bucket, Split}, Key) ->
    SplitSz = size(Split),
    BucketSz = size(Bucket),
    <<?VERSION_3:7, 0:1, SplitSz:16/integer, Split/binary, BucketSz:16/integer, Bucket/binary, Key/binary>>.




%% Make the riak key from the bitcask key, based on the version number
make_riak_key(<<?VERSION_3:7, HasType:1, SplitSz:16/integer, Split:SplitSz/bytes, Sz:16/integer,
    TypeOrBucket:Sz/bytes, Rest/binary>>) ->
    case HasType of
        0 ->
            %% no type, first field is bucket
            {Split, TypeOrBucket, Rest};
        1 ->
            %% has a tyoe, extract bucket as well
            <<BucketSz:16/integer, Bucket:BucketSz/bytes, Key/binary>> = Rest,
            {Split, {TypeOrBucket, Bucket}, Key}
    end;
make_riak_key(<<?VERSION_2:7, HasType:1, Sz:16/integer,
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
make_riak_key(<<?VERSION_1:7, HasType:1, Sz:16/integer,
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
make_riak_key(<<?VERSION_0:8,_Rest/binary>> = BK) ->
    binary_to_term(BK).




%% Take the bitcask key stored down to disk, and decode it to match the keydir stored in memory
%% return the keyinfo record, to find additional information that was stored down in the encoding
decode_disk_key(<<Version:7, Type:1, TstampExpire:32/integer, Rest/binary>>) when Version =:= ?VERSION_3 orelse Version =:= ?VERSION_2 ->
    #keyinfo{
        key = <<?CURRENT_VERSION:7, Type:1, Rest/binary>>,
        tstamp_expire = TstampExpire
    };

decode_disk_key(<<?VERSION_1:7, 0:1, BucketSz:16/integer, Bucket:BucketSz/binary, Key/binary>>) ->
    #keyinfo{
        key = make_bitcask_key(?CURRENT_VERSION, Bucket, Key)
    };

decode_disk_key(<<?VERSION_1:7, 1:1, TypeSz:16/integer, Type:TypeSz/binary, BucketSz:16/integer, Bucket:BucketSz/binary, Key/binary>>) ->
    #keyinfo{
        key = make_bitcask_key(?CURRENT_VERSION, {Type, Bucket}, Key)
    };

decode_disk_key(<<?VERSION_0:8,_Rest/bits>> = Key0) ->
    {Bucket, Key} = binary_to_term(Key0),
    #keyinfo{
        key = make_bitcask_key(?CURRENT_VERSION, Bucket, Key)
    }.




%% Take the bitcask key generated by make_bitask_key, and encode it with additional information to be stored down to
%% disk, but not stored in the keydir.
encode_disk_key(<<Version:7, Type:1, Rest/bits>>, Opts) when Version =:= ?VERSION_2 orelse Version =:= ?VERSION_3 ->
    TstampExpire = proplists:get_value(?TSTAMP_EXPIRE_KEY, Opts, ?DEFAULT_TSTAMP_EXPIRE),
    <<Version:7, Type:1, TstampExpire:32/integer, Rest/bits>>;

encode_disk_key(<<?VERSION_1:7, _Rest/bits>> = BitcaskKey, _Opts) ->
    BitcaskKey;

encode_disk_key(<<?VERSION_0:8,_Rest/bits>> = BitcaskKey, _Opts) ->
    BitcaskKey.

-spec find_split(riak_object:bucket() | riak_object:key(), atom()) -> atom().
find_split(Key, undefined) ->
    case make_riak_key(Key) of
        {Split, {_Type, _Bucket}, _Key} ->
            binary_to_atom(Split, latin1);
        {Split, _Bucket, _Key} ->
            binary_to_atom(Split, latin1);
        {{_Type, Bucket}, _Key} ->
            binary_to_atom(Bucket, latin1);
        {Bucket, _Key} ->
            binary_to_atom(Bucket, latin1);
        _ ->
            default
    end;
find_split(Key0, Partition) when is_integer(Partition) ->
    case make_riak_key(Key0) of
        {_Split, {_Type, Bucket}, Key} ->
            Split1 = get_split(Bucket, Key, Partition, find_split),
            binary_to_atom(Split1, latin1);
        {_Split, Bucket, Key} ->
            Split1 = get_split(Bucket, Key, Partition, find_split),
            binary_to_atom(Split1, latin1);
        {{_Type, Bucket}, Key} ->
            Split1 = get_split(Bucket, Key, Partition, find_split),
            binary_to_atom(Split1, latin1);
        {Bucket, Key} ->
            Split1 = get_split(Bucket, Key, Partition, find_split),
            binary_to_atom(Split1, latin1);
        _ ->
            default
    end.

%%check_and_upgrade_key(default, <<Version:7, _Rest/bitstring>> = KeyDirKey) when Version =/= ?VERSION_3 ->
%%    KeyDirKey;
-spec check_and_upgrade_key(atom(), binary()) -> binary().
check_and_upgrade_key(Split, <<Version:7, _Rest/bitstring>> = KeyDirKey) when Version =/= ?VERSION_3 ->
    case ?DECODE_BITCASK_KEY(KeyDirKey) of
        {{Bucket, Type}, Key} ->
            ?ENCODE_BITCASK_KEY(3, {Type, Bucket, atom_to_binary(Split, latin1)}, Key);
        {Bucket, Key} ->
            ?ENCODE_BITCASK_KEY(3, {Bucket, atom_to_binary(Split, latin1)}, Key)
    end;
check_and_upgrade_key(Split0, <<?VERSION_3:7, _Rest/bitstring>> = KeyDirKey) ->
    case ?DECODE_BITCASK_KEY(KeyDirKey) of
        {Split0, {_Type, _Bucket}, _Key} ->
            KeyDirKey;
        {_Split1, {Type, Bucket}, Key} ->
            ?ENCODE_BITCASK_KEY(3, {Type, Bucket, atom_to_binary(Split0, latin1)}, Key);
        {Split0, _Bucket, _Key} ->
            KeyDirKey;
        {_Split1, Bucket, Key} ->
            ?ENCODE_BITCASK_KEY(3, {Bucket, atom_to_binary(Split0, latin1)}, Key)
    end;
check_and_upgrade_key(_Split, KeyDirKey) ->
    KeyDirKey.


%% ===================================================================
%% Internal functions
%% ===================================================================

% @private If no pending merges, check if files need to be merged.
merge_check(Ref, BitcaskRoot, BitcaskOpts) ->
    case bitcask_merge_worker:status() of
        {0, _} ->
            MaxMergeSize = app_helper:get_env(riak_kv,
                                              bitcask_max_merge_size),
%%            BState = erlang:get(Ref),
%%            {default, _DefRef, _, _} = lists:keyfind(default, 1, element(2, BState)),
%%            DefState = erlang:get(DefRef),


            case bitcask_manager:needs_merge(Ref, [{max_merge_size, MaxMergeSize}]) of
                {true, Files} ->
                    bitcask_merge_worker:merge(BitcaskRoot, BitcaskOpts, Files);
                false ->
                    ok
            end;
        _ ->
            lager:debug("Skipping merge check: Pending merges"),
            ok
    end.

%% @private
%% On linux there is a kernel bug that won't allow fcntl to add O_SYNC
%% to an already open file descriptor.
check_fcntl() ->
    Logged=application:get_env(riak_kv,o_sync_warning_logged),
    Strategy=application:get_env(bitcask,sync_strategy),
    case {Logged,Strategy} of
        {undefined,{ok,o_sync}} ->
            case riak_core_util:is_arch(linux) of
                true ->
                    lager:warning("{sync_strategy,o_sync} not implemented on Linux"),
                    application:set_env(riak_kv,o_sync_warning_logged,true);
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

%% @private
%% Return a function to fold over the buckets on this backend
fold_buckets_fun(FoldBucketsFun) ->
    fun(#bitcask_entry{key=BK}, {Acc, BucketSet}) ->
            case make_riak_key(BK) of
                {Bucket, _} ->
                    case sets:is_element(Bucket, BucketSet) of
                        true ->
                            {Acc, BucketSet};
                        false ->
                            {FoldBucketsFun(Bucket, Acc),
                                sets:add_element(Bucket, BucketSet)}
                    end;
                {_S, Bucket, _} ->
                    case sets:is_element(Bucket, BucketSet) of
                        true ->
                            {Acc, BucketSet};
                        false ->
                            {FoldBucketsFun(Bucket, Acc),
                                sets:add_element(Bucket, BucketSet)}
                    end
            end
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_keys_fun(FoldKeysFun, <<"undefined">>) ->
    fun(#bitcask_entry{key=BK}, Acc) ->
        case make_riak_key(BK) of
            {_S, Bucket, Key} ->
                FoldKeysFun(Bucket, Key, Acc);
            {Bucket, Key} ->
                FoldKeysFun(Bucket, Key, Acc)
        end
    end;
fold_keys_fun(FoldKeysFun, Bucket) ->
    fun(#bitcask_entry{key=BK}, Acc) ->
        case make_riak_key(BK) of
            {_S, B, Key} ->
                case B =:= Bucket of
                    true ->
                        FoldKeysFun(B, Key, Acc);
                    false ->
                        Acc
                end;
            {B, Key} ->
                case B =:= Bucket of
                    true ->
                        FoldKeysFun(B, Key, Acc);
                    false ->
                        Acc
                end
        end
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_objects_fun(FoldObjectsFun, <<"undefined">>) ->
    fun(BK, Value, Acc) ->
            case make_riak_key(BK) of
                {_S, Bucket, Key} ->
                    FoldObjectsFun(Bucket, Key, Value, Acc);
                {Bucket, Key} ->
                    FoldObjectsFun(Bucket, Key, Value, Acc)
            end
    end;
fold_objects_fun(FoldObjectsFun, Bucket) ->
    fun(BK, Value, Acc) ->
        case make_riak_key(BK) of
            {B, Key} ->
                case B =:= Bucket of
                    true ->
                        FoldObjectsFun(B, Key, Value, Acc);
                    false ->
                        Acc
                end;
            {_S, B, Key} ->
                case B =:= Bucket of
                    true ->
                        FoldObjectsFun(B, Key, Value, Acc);
                    false ->
                        Acc
                end
        end
    end.

%% @private
%% Schedule sync (if necessary)
maybe_schedule_sync(Ref) when is_reference(Ref) ->
    case application:get_env(bitcask, sync_strategy) of
        {ok, {seconds, Seconds}} ->
            SyncIntervalMs = timer:seconds(Seconds),
            schedule_sync(Ref, SyncIntervalMs);
        %% erlang:send_after(SyncIntervalMs, self(),
        %%                   {?MODULE, {sync, SyncIntervalMs}});
        {ok, none} ->
            ok;
        {ok, o_sync} ->
            ok;
        BadStrategy ->
            lager:notice("Ignoring invalid bitcask sync strategy: ~p",
                         [BadStrategy]),
            ok
    end.

schedule_sync(Ref, SyncIntervalMs) when is_reference(Ref) ->
    riak_kv_backend:callback_after(SyncIntervalMs, Ref, {sync, SyncIntervalMs}).

schedule_merge(Ref) when is_reference(Ref) ->
    Interval = app_helper:get_env(riak_kv, bitcask_merge_check_interval,
                                  ?MERGE_CHECK_INTERVAL),
    JitterPerc = app_helper:get_env(riak_kv, bitcask_merge_check_jitter,
                                    ?MERGE_CHECK_JITTER),
    Jitter = Interval * JitterPerc,
    FinalInterval = Interval + trunc(2 * rand:uniform() * Jitter - Jitter),
    lager:debug("Scheduling Bitcask merge check in ~pms", [FinalInterval]),
    riak_kv_backend:callback_after(FinalInterval, Ref, merge_check).

-spec schedule_upgrade_check(reference()) -> reference().
schedule_upgrade_check(Ref) when is_reference(Ref) ->
    riak_kv_backend:callback_after(?UPGRADE_CHECK_INTERVAL, Ref,
                                   upgrade_check).

-spec maybe_schedule_upgrade_check(reference(),
                                   {upgrading, version()} | no_upgrade) -> ok.
maybe_schedule_upgrade_check(Ref, {upgrading, _}) ->
    _ = schedule_upgrade_check(Ref),
    ok;
maybe_schedule_upgrade_check(_Ref, no_upgrade) ->
    ok.

%% @private
get_data_dir(DataRoot, Partition) ->
    PartitionDir = filename:join([DataRoot, Partition]),
    make_data_dir(PartitionDir).

%% @private
make_data_dir(PartitionFile) ->
    AbsPath = filename:absname(PartitionFile),
    DataDir = filename:basename(PartitionFile),
    case filelib:ensure_dir(filename:join([AbsPath, dummy])) of
        ok ->
            {ok, DataDir};
        {error, Reason} ->
            lager:error("Failed to create bitcask dir ~s: ~p",
                        [DataDir, Reason]),
            {error, Reason}
    end.

%% @private
data_directory_cleanup(DirPath) ->
    case file:list_dir(DirPath) of
        {ok, Files} ->
%%            Delete = [file:delete(filename:join([DirPath, File])) || File <- Files],
            lists:foreach(
                fun(File) ->
                    case filelib:is_dir(filename:join([DirPath, File])) of
                        false ->
                            file:delete(filename:join([DirPath, File]));
                        true ->
                            case file:list_dir(filename:join([DirPath, File])) of
                                {ok, Files1} ->
                                    [file:delete(filename:join([DirPath, File, File1])) || File1 <- Files1],
                                    file:del_dir(filename:join([DirPath, File]));
                                _ ->
                                    ignore
                            end
                    end
                end, Files),

            file:del_dir(DirPath);
        _ ->
            ignore
    end.

%% @private
set_mode(read_only, Config) ->
    Config1 = lists:keystore(read_only, 1, Config, {read_only, true}),
    lists:keydelete(read_write, 1, Config1);
set_mode(read_write, Config) ->
    Config1 = lists:keystore(read_write, 1, Config, {read_write, true}),
    lists:keydelete(read_only, 1, Config1).

-spec read_version(File::string()) -> undefined | version().
read_version(File) ->
    case file:read_file(File) of
        {ok, VsnContents} ->
            % Crude parser to ignore common user introduced variations
            VsnLines = [binary:replace(B, [<<" ">>, <<"\t">>], <<>>, [global])
                        || B <- binary:split(VsnContents,
                                             [<<"\n">>, <<"\r">>, <<"\r\n">>])],
            VsnLine = [L || L <- VsnLines, L /= <<>>],
            case VsnLine of
                [VsnStr] ->
                    try
                        version_from_str(binary_to_list(VsnStr))
                    catch
                        _:_ ->
                            undefined
                    end;
                _ ->
                    undefined
            end;
        {error, _} ->
            undefined
    end.

-spec bitcask_files(string()) -> {ok, [string()]} | {error, term()}.
bitcask_files(Dir) ->
    case file:list_dir(Dir) of
        {ok, Files} ->
            BitcaskFiles = [F || F <- Files, lists:suffix(".bitcask.data", F)],
            {ok, BitcaskFiles};
        {error, Err} ->
            {error, Err}
    end.

-spec has_bitcask_files(string()) -> boolean() | {error, term()}.
has_bitcask_files(Dir) ->
    case bitcask_files(Dir) of
        {ok, Files} ->
            case Files of
                [] -> false;
                _ -> true
            end;
        {error, Err} ->
            {error, Err}
    end.

-spec needs_upgrade(version() | undefined, version()) -> boolean().
needs_upgrade(undefined, _) ->
    true;
needs_upgrade({A1, B1, _}, {A2, B2, _})
  when {A1, B1} =< {1, 6}, {A2, B2} > {1, 6} ->
    true;
needs_upgrade(_, _) ->
    false.

-spec maybe_start_upgrade(string()) -> no_upgrade | {upgrading, version()}.
maybe_start_upgrade(Dir) ->
    % Are we already upgrading?
    UpgradeFile = filename:join(Dir, ?UPGRADE_FILE),
    UpgradingVsn = read_version(UpgradeFile),
    case UpgradingVsn of
        undefined ->
            % No, maybe if previous data needs upgrade
            maybe_start_upgrade_if_bitcask_files(Dir);
        _ ->
            lager:info("Continuing upgrade to version ~s in ~s",
                       [version_to_str(UpgradingVsn), Dir]),
            {upgrading, UpgradingVsn}
    end.

-spec maybe_start_upgrade_if_bitcask_files(string()) ->
    no_upgrade | {upgrading, version()}.
maybe_start_upgrade_if_bitcask_files(Dir) ->
    NewVsn = bitcask_version(),
    VersionFile = filename:join(Dir, ?VERSION_FILE),
    CurrentVsn = read_version(VersionFile),
    case has_bitcask_files(Dir) of
        true ->
            case needs_upgrade(CurrentVsn, NewVsn) of
                true ->
                    NewVsnStr = version_to_str(NewVsn),
                    case start_upgrade(Dir, CurrentVsn, NewVsn) of
                        ok ->
                            UpgradeFile = filename:join(Dir, ?UPGRADE_FILE),
                            write_version(UpgradeFile, NewVsn),
                            lager:info("Starting upgrade to version ~s in ~s",
                                       [NewVsnStr, Dir]),
                            {upgrading, NewVsn};
                        {error, UpgradeErr} ->
                            lager:error("Failed to start upgrade to version ~s"
                                        " in ~s : ~p",
                                        [NewVsnStr, Dir, UpgradeErr]),
                            no_upgrade
                    end;
                false ->
                    case CurrentVsn == NewVsn of
                        true ->
                            no_upgrade;
                        false ->
                            write_version(VersionFile, NewVsn),
                            no_upgrade
                    end
            end;
        false ->
            write_version(VersionFile, NewVsn),
            no_upgrade;
        {error, Err} ->
            lager:error("Failed to check for bitcask files in ~s."
                        " Can not determine if an upgrade is needed : ~p",
                        [Dir, Err]),
            no_upgrade
    end.

% @doc Start the upgrade process for the given old/new version pair.
-spec start_upgrade(string(), version() | undefined, version()) ->
    ok | {error, term()}.
start_upgrade(Dir, OldVsn, NewVsn)
  when OldVsn < {1, 7, 0}, NewVsn >= {1, 7, 0} ->
    % NOTE: The guard handles old version being undefined, as atom < tuple
    % That is always the case with versions < 1.7.0 anyway.
    case bitcask_files(Dir) of
        {ok, Files} ->
            % Write merge.txt with a list of all bitcask files to merge
            MergeFile = filename:join(Dir, ?MERGE_FILE),
            MergeList = to_merge_list(Files, ?UPGRADE_MERGE_BATCH_SIZE),
            case riak_core_util:replace_file(MergeFile, MergeList) of
                ok ->
                    ok;
                {error, WriteErr} ->
                    {error, WriteErr}
            end;
        {error, BitcaskReadErr} ->
            {error, BitcaskReadErr}
    end.

% @doc Transform to contents of merge.txt, with an empty line every
% Batch files, which delimits the merge batches.
-spec to_merge_list([string()], pos_integer()) -> iodata().
to_merge_list(L, Batch) ->
    N = length(L),
    LN = lists:zip(L, lists:seq(1, N)),
    [case I rem Batch == 0 of
         true ->
             [E, "\n\n"];
         false ->
             [E, "\n"]
     end || {E, I} <- LN].

%% @doc Checks progress of an ongoing backend upgrade.
-spec check_upgrade(Dir :: string()) -> pending | finished | {error, term()}.
check_upgrade(Dir) ->
    UpgradeFile = filename:join(Dir, ?UPGRADE_FILE),
    case filelib:is_regular(UpgradeFile) of
        true ->
            % Look for merge file. When all merged, Bitcask deletes it.
            MergeFile = filename:join(Dir, ?MERGE_FILE),
            case filelib:is_regular(MergeFile) of
                true ->
                    pending;
                false ->
                    finished
            end;
        false ->
            % Flattening so it prints like a string
            Reason = lists:flatten(io_lib:format("Missing upgrade file ~s",
                                                 [UpgradeFile])),
            {error, Reason}
    end.

-spec version_from_str(string()) -> version().
version_from_str(VsnStr) ->
    [Major, Minor, Patch] =
        [list_to_integer(Tok) || Tok <- string:tokens(VsnStr, ".")],
    {Major, Minor, Patch}.

-spec bitcask_version() -> version().
bitcask_version() ->
    version_from_str(bitcask_version_str()).

-spec bitcask_version_str() -> string().
bitcask_version_str() ->
    Apps = application:which_applications(),
    % For tests, etc without the app, use big version number to avoid upgrades
    BitcaskVsn = hd([Vsn || {bitcask, _, Vsn} <- Apps] ++ ["999.999.999"]),
    BitcaskVsn.

-spec version_to_str(version()) -> string().
version_to_str({Major, Minor, Patch}) ->
    io_lib:format("~p.~p.~p", [Major, Minor, Patch]).

-spec write_version(File::string(), Vsn::string() | version()) ->
    ok | {error, term()}.
write_version(File, {_, _, _} = Vsn) ->
    write_version(File, version_to_str(Vsn));
write_version(File, Vsn) ->
    riak_core_util:replace_file(File, Vsn).

-spec finalize_upgrade(Dir :: string()) -> {ok, version()} | {error, term()}.
finalize_upgrade(Dir) ->
    UpgradeFile = filename:join(Dir, ?UPGRADE_FILE),
    case read_version(UpgradeFile) of
        undefined ->
            {error, no_upgrade_version};
        Vsn ->
            VsnFile = filename:join(Dir, ?VERSION_FILE),
            case write_version(VsnFile, Vsn) of
                ok ->
                    case file:delete(UpgradeFile) of
                        ok ->
                            {ok, Vsn};
                        {error, DelErr} ->
                            {error, DelErr}
                    end;
                {error, WriteErr} ->
                    {error, WriteErr}
            end
    end.

-spec add_split_opts(riak_object:bucket(), config(), integer()) -> config().
add_split_opts(Bucket, Opts, Partition) ->
    case Bucket of
        <<"undefined">> ->
            Opts;
        _ ->
            check_md(Bucket, Opts, Partition)
    end.

-spec check_md(riak_object:bucket(), config(), integer()) -> config().
-ifdef(TEST).
check_md(<<"second_split">> = Bucket, Opts, _Partition) ->
    [{split, binary_to_atom(Bucket, latin1)} | Opts];
check_md(_, Opts, _Partition) ->
    Opts.
-else.
check_md(Bucket, Opts, Partition) ->
    case riak_core_metadata:get({split_backend, splits}, {Bucket, node()}) of
        undefined ->
            Opts;
        BackendStates ->
            case lists:keyfind(Partition, 1, BackendStates) of
                false ->
                    Opts;
                {Partition, _} ->
                    [{split, binary_to_atom(Bucket, latin1)} | Opts]
            end
    end.
-endif.

-spec get_split(riak_object:bucket(), riak_object:key(), integer(), atom()) -> riak_object:bucket() | riak_object:key().
-ifdef(TEST).
%% Partition value 0 represents a false state in MD for testing. Partition value 1 represents active or special_merge.
get_split(<<"second_split">>, _, 0, get) ->
    <<"second_split">>;
get_split(<<"second_split">>, _, 0, _) ->
    <<"default">>;
get_split(<<"second_split">>, _, 1, _) ->
    <<"second_split">>;
get_split(_, _, _, _) ->
    <<"default">>.
-else.
get_split(Bucket, Key, Partition, Op) ->
    case get_md_state(Bucket, Key, Partition) of
        undefined ->
            <<"default">>;
        {Split, active} ->
            Split;
        {Split, special_merge} ->
            Split;
        {Split, false} ->
            case Op of
                get ->
                    Split;
                _ ->
                    <<"default">>
            end
    end.

-spec get_md_state(riak_object:bucket(), riak_object:key(), integer()) -> {binary(), atom()} | undefined.
get_md_state(Bucket, Key, Partition) ->
    case riak_core_metadata:get({split_backend, splits}, {Bucket, node()}) of
        undefined ->
            case riak_core_metadata:get({split_backend, splits}, {Key, node()}) of
                undefined ->
                    undefined;
                BackendStates ->
                    case lists:keyfind(Partition, 1, BackendStates) of
                        false ->
                            undefined;
                        {Partition, State} ->
                            {Key, State}
                    end
            end;
        BackendStates ->
            case lists:keyfind(Partition, 1, BackendStates) of
                false ->
                    undefined;
                {Partition, State} ->
                    {Bucket, State}
            end
    end.
-endif.
%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_test_() ->
    Path = riak_kv_test_util:get_test_dir("bitcask-backend"),
    ?assertCmd("rm -rf " ++ Path ++ "/*"),
    application:set_env(bitcask, data_root, ""),
    riak_core_metadata_manager:start_link([{data_dir, "test/md/bitcask-backend"}]),
    backend_test_util:standard_test_gen(?MODULE,
                                        [{data_root, Path}]).

custom_config_test_() ->
    Path = riak_kv_test_util:get_test_dir("bitcask-backend"),
    ?assertCmd("rm -rf " ++ Path ++ "/*"),
    application:set_env(bitcask, data_root, ""),
    riak_core_metadata_manager:start_link([{data_dir, "test/md/bitcask-backend"}]),
    backend_test_util:standard_test_gen(?MODULE,
                                        [{data_root, Path}]).

startup_data_dir_test() ->
    Path = riak_kv_test_util:get_test_dir("bitcask-backend"),
    ?assertCmd("rm -rf " ++ Path ++ "/*"),
    Config = [{data_root, Path}],
    %% Start the backend
    {ok, State} = start(42, Config),
    %% Stop the backend
    ok = stop(State),
    %% Ensure the timestamped directories have been moved
    {ok, DataDirs} = file:list_dir(Path),
    ?assertCmd("rm -rf " ++ Path),
    ?assertEqual(["42"], DataDirs).

drop_test() ->
    Path = riak_kv_test_util:get_test_dir("bitcask-backend"),
    ?assertCmd("rm -rf " ++ Path ++ "/*"),
    Config = [{data_root, Path}],
    %% Start the backend
    {ok, State} = start(42, Config),
    %% Drop the backend
    {ok, State1} = drop(State),
    %% Ensure the timestamped directories have been moved
    {ok, DataDirs} = file:list_dir(Path),
    %% RemovalPartitionDirs = lists:reverse(TSPartitionDirs),
    %% Stop the backend
    ok = stop(State1),
    ?assertCmd("rm -rf " ++ Path),
    ?assertEqual([], DataDirs).

get_data_dir_test() ->
    %% Cleanup
    Path = riak_kv_test_util:get_test_dir("bitcask-backend"),
    ?assertCmd("rm -rf " ++ Path ++ "/*"),
    %% Create a set of timestamped partition directories
    %% plus some base directories for other partitions
    TSPartitionDirs =
        [filename:join(["21-" ++ integer_to_list(X)]) ||
                          X <- lists:seq(1, 10)],
    OtherPartitionDirs = [integer_to_list(X) || X <- lists:seq(1,10)],
    [filelib:ensure_dir(filename:join([Path, Dir, dummy]))
     || Dir <- TSPartitionDirs ++ OtherPartitionDirs],
    %% Check the results
    ?assertEqual({ok, "21"}, get_data_dir(Path, "21")).

key_version_test() ->
    FoldKeysFun =
        fun(Bucket, Key, Acc) ->
                [{Bucket, Key} | Acc]
        end,

    Path = riak_kv_test_util:get_test_dir("bitcask-backend"),
    ?assertCmd("rm -rf " ++ Path ++ "/*"),
    application:set_env(bitcask, data_root, Path),
    application:set_env(bitcask, small_keys, true),
    {ok, S} = ?MODULE:start(42, []),
    ?MODULE:put(<<"b1">>, <<"k1">>, [], <<"v1">>, S),
    ?MODULE:put(<<"b2">>, <<"k1">>, [], <<"v2">>, S),
    ?MODULE:put(<<"b3">>, <<"k1">>, [], <<"v3">>, S),

    ?assertMatch({ok, <<"v2">>, _}, ?MODULE:get(<<"b2">>, <<"k1">>, S)),

    ?MODULE:stop(S),
    application:set_env(bitcask, small_keys, false),
    {ok, S1} = ?MODULE:start(42, []),
    %%{ok, L0} = ?MODULE:fold_keys(FoldKeysFun, [], [], S1),
    %%io:format("~p~n", [L0]),

    ?assertMatch({ok, <<"v2">>, _}, ?MODULE:get(<<"b2">>, <<"k1">>, S1)),

    ?MODULE:put(<<"b4">>, <<"k1">>, [], <<"v4">>, S1),
    ?MODULE:put(<<"b5">>, <<"k1">>, [], <<"v5">>, S1),

    ?MODULE:stop(S1),
    application:set_env(bitcask, small_keys, true),
    {ok, S2} = ?MODULE:start(42, []),

    {ok, L0} = ?MODULE:fold_keys(FoldKeysFun, [], [], S2),
    L = lists:sort(L0),
    riak_kv_test_util:stop_process(riak_core_metadata_manager),
    riak_kv_test_util:stop_process(riak_core_metadata_events),
    ?_assertEqual([
                   {<<"b1">>, <<"k1">>},
                   {<<"b2">>, <<"k1">>},
                   {<<"b3">>, <<"k1">>},
                   {<<"b4">>, <<"k1">>},
                   {<<"b5">>, <<"k1">>}
                  ],
                  L).
%% In the below tests when the state is updated to alter the partition number to 1 this is mock a call to md to return the correct split.
create_new_split_test() ->
    os:cmd("rm -rf test/bitcask-backend/*"),
    Opts = [{data_root, "test/bitcask-backend"}, {}],
    {ok, S} = ?MODULE:start(0, Opts),

    Path = filename:join(["test/bitcask-backend", "0"]),
    {ok, DataDirs} = file:list_dir(Path),
    Dirs = [Dir || Dir <- DataDirs, Dir =/= "version.txt"],

    ?assertEqual(["default"], Dirs),

    {ok, S1} = ?MODULE:start_additional_split({second, false}, S),

    {ok, DataDirs1} = file:list_dir(Path),
    Dirs1 = [Dir || Dir <- DataDirs1, Dir =/= "version.txt"],

    ?assertEqual(["default", "second"], Dirs1),
    ?assertEqual(true, check_backend_exists(second, S1)),
    ?assertEqual(false, is_backend_active(second, S1)),

    ok = stop(S1),
    os:cmd("rm -rf test/bitcask-backend/*").

split_data_test() ->
    os:cmd("rm -rf test/bitcask-backend/*"),
    Opts = [{data_root, "test/bitcask-backend"}, {}],
    {ok, S} = ?MODULE:start(0, Opts),
    {ok, S1} = ?MODULE:start_additional_split({second_split, false}, S),

    ?assertEqual(true, check_backend_exists(default, S1)),
    ?assertEqual(true, check_backend_exists(second_split, S1)),
    ?assertEqual(true, is_backend_active(default, S1)),
    ?assertEqual(false, is_backend_active(second_split, S1)),

    ?MODULE:put(<<"b1">>, <<"k1">>, [], <<"v1">>, S1),
    ?MODULE:put(<<"b2">>, <<"k1">>, [], <<"v2">>, S1),
    ?MODULE:put(<<"second_split">>, <<"k1">>, [], <<"v3">>, S1),

    {ok, <<"v1">>, _} = ?MODULE:get(<<"b1">>, <<"k1">>, S1),
    {ok, <<"v2">>, _} = ?MODULE:get(<<"b2">>, <<"k1">>, S1),
    {ok, <<"v3">>, _} = ?MODULE:get(<<"second_split">>, <<"k1">>, S1),

    Path = filename:join(["test/bitcask-backend", "0"]),
    {ok, DataDirs} = file:list_dir(Path),
    Dirs = [Dir || Dir <- DataDirs, Dir =/= "version.txt"],
    ?assertEqual(["default", "second_split"], Dirs),
    ?assertEqual({ok, []}, file:list_dir("test/bitcask-backend/0/second_split")),

    ?MODULE:activate_backend(second_split, S1),
    ?assertEqual(true, is_backend_active(second_split, S1)),

    ?MODULE:put(<<"second_split">>, <<"k4">>, [], <<"v4">>, S1#state{partition = 1}),
    {ok, <<"v4">>, _} = ?MODULE:get(<<"second_split">>, <<"k4">>, S1#state{partition = 1}),

    {ok, SplitFiles} = file:list_dir("test/bitcask-backend/0/second_split"),

    ?assertNotEqual([], SplitFiles),

    ok = stop(S1),
    os:cmd("rm -rf test/bitcask-backend/*").

special_merge_test() ->
    os:cmd("rm -rf test/bitcask-backend/*"),
    Opts = [{data_root, "test/bitcask-backend"}, {tombstone_version, 2}],
    {ok, S} = ?MODULE:start(0, Opts),
    {ok, S1} = ?MODULE:start_additional_split({second_split, false}, S),

    ?assertEqual(true, check_backend_exists(default, S1)),
    ?assertEqual(true, check_backend_exists(second_split, S1)),
    ?assertEqual(true, is_backend_active(default, S1)),
    ?assertEqual(false, is_backend_active(second_split, S1)),

    ?MODULE:put(<<"b1">>, <<"k1">>, [], <<"v1">>, S1),
    ?MODULE:put(<<"b2">>, <<"k2">>, [], <<"v2">>, S1),
    ?MODULE:put(<<"second_split">>, <<"k1">>, [], <<"v1">>, S1),
    ?MODULE:put(<<"second_split">>, <<"k2">>, [], <<"v2">>, S1),

    ?MODULE:activate_backend(second_split, S1),
    ?assertEqual(true, is_backend_active(second_split, S1)),

    Path = filename:join(["test/bitcask-backend", "0"]),
    {ok, DataDirs} = file:list_dir(Path),
    Dirs = [Dir || Dir <- DataDirs, Dir =/= "version.txt"],
    ?assertEqual(["default", "second_split"], Dirs),
    ?assertEqual({ok, []}, file:list_dir("test/bitcask-backend/0/second_split")),

    ?MODULE:special_merge(default, second_split, S1#state{partition = 1}),

    ?assertNotEqual({ok, []}, file:list_dir("test/bitcask-backend/0/second_split")),

    ?MODULE:activate_backend(second_split, S1),

    ?MODULE:special_merge(default, second_split, S1#state{partition = 1}),

    ok = stop(S1),
    os:cmd("rm -rf test/bitcask-backend/*").

split_merge_test() ->
    os:cmd("rm -rf test/bitcask-backend/*"),
    Opts = [{data_root, "test/bitcask-backend"}, {max_file_size, 10}],
    {ok, S} = ?MODULE:start(0, Opts),
    {ok, S1} = ?MODULE:start_additional_split({second_split, false}, S),

    ?assertEqual(true, check_backend_exists(default, S1)),
    ?assertEqual(true, check_backend_exists(second_split, S1)),
    ?assertEqual(true, is_backend_active(default, S1)),
    ?assertEqual(false, is_backend_active(second_split, S1)),

    ?MODULE:put(<<"b1">>, <<"k1">>, [], <<"v1">>, S1),
    ?MODULE:put(<<"b2">>, <<"k2">>, [], <<"v2">>, S1),
    ?MODULE:put(<<"second_split">>, <<"k1">>, [], <<"v1">>, S1),
    ?MODULE:put(<<"second_split">>, <<"k2">>, [], <<"v2">>, S1),

    ?MODULE:activate_backend(second_split, S1),
    ?assertEqual(true, is_backend_active(second_split, S1)),

    Path = filename:join(["test/bitcask-backend", "0"]),
    {ok, DataDirs} = file:list_dir(Path),
    Dirs = [Dir || Dir <- DataDirs, Dir =/= "version.txt"],
    ?assertEqual(["default", "second_split"], Dirs),
    ?assertEqual({ok, []}, file:list_dir("test/bitcask-backend/0/second_split")),

    ?MODULE:put(<<"second_split">>, <<"k1">>, [], <<"v1">>, S1#state{partition = 1}),
    ?MODULE:put(<<"second_split">>, <<"k2">>, [], <<"v2">>, S1#state{partition = 1}),

    ?assertNotEqual({ok, []}, file:list_dir("test/bitcask-backend/0/second_split")),

    {ok, BeforeMerge} = file:list_dir("test/bitcask-backend/0/default"),
    BRef = S1#state.ref,

    false = bitcask_manager:needs_merge(BRef),
    ?MODULE:special_merge(default, second_split, S1#state{partition = 1}),
    {true, MFiles} = bitcask_manager:needs_merge(BRef),

    BitcaskRoot = filename:join(S1#state.root, S1#state.data_dir),
    ok = bitcask_merge_worker:merge(BitcaskRoot, [], MFiles),
    timer:sleep(2000),

    {ok, AfterMerge} = file:list_dir("test/bitcask-backend/0/default"),
    ?assert(length(AfterMerge) < length(BeforeMerge)),

    ok = stop(S1),
    os:cmd("rm -rf test/bitcask-backend/*").

split_fold_keys_test() ->
    ct:pal("########################### Split_FOLD_KEYS_TEST ##################"),
    os:cmd("rm -rf test/bitcask-backend/*"),
    Opts = [{data_root, "test/bitcask-backend"}, {}],
    {ok, S} = ?MODULE:start(0, Opts),
    {ok, S1} = ?MODULE:start_additional_split({second_split, false}, S),

    BufferMod = riak_kv_fold_buffer,

    FoldFun = fun(_, Key, Buffer) ->
        BufferMod:add(Key, Buffer)
              end,

    ResultFun =
        fun(_Items) ->
            ok
        end,

    BufferAcc = BufferMod:new(10, ResultFun),

    ?assertEqual(true, check_backend_exists(default, S1)),
    ?assertEqual(true, check_backend_exists(second_split, S1)),
    ?assertEqual(true, is_backend_active(default, S1)),
    ?assertEqual(false, is_backend_active(second_split, S1)),

    ?MODULE:put(<<"b1">>, <<"k1">>, [], <<"v1">>, S1),
    ?MODULE:put(<<"b1">>, <<"k2">>, [], <<"v2">>, S1),
    ?MODULE:put(<<"second_split">>, <<"k3">>, [], <<"v1">>, S1),
    ?MODULE:put(<<"second_split">>, <<"k4">>, [], <<"v2">>, S1),

    {async, AsyncBuff} = ?MODULE:fold_keys(FoldFun, BufferAcc, [async_fold, {bucket, <<"b1">>}], S1),
    {async, AsyncBuff1} = ?MODULE:fold_keys(FoldFun, BufferAcc, [async_fold, {bucket, <<"second_split">>}], S1),

    {ok, Buff} = ?MODULE:fold_keys(FoldFun, BufferAcc, [{bucket, <<"b1">>}], S1),
    {ok, Buff1} = ?MODULE:fold_keys(FoldFun, BufferAcc, [{bucket, <<"second_split">>}], S1),

    AsyncKeys  = element(2, AsyncBuff()),
    AsyncKeys1 = element(2, AsyncBuff1()),
    Keys  = element(2, Buff),
    Keys1 = element(2, Buff1),

    ?assertEqual([<<"k1">>, <<"k2">>], lists:sort(AsyncKeys)),
    ?assertEqual([<<"k1">>, <<"k2">>], lists:sort(Keys)),
    ?assertEqual([<<"k3">>, <<"k4">>], lists:sort(AsyncKeys1)),
    ?assertEqual([<<"k3">>, <<"k4">>], lists:sort(Keys1)),

    ?MODULE:activate_backend(second_split, S1),
    ?assertEqual(true, is_backend_active(second_split, S1)),

    ?MODULE:put(<<"second_split">>, <<"k5">>, [], <<"v1">>, S1#state{partition = 1}),
    ?MODULE:put(<<"second_split">>, <<"k6">>, [], <<"v2">>, S1#state{partition = 1}),

    {async, AsyncBuff2} = ?MODULE:fold_keys(FoldFun, BufferAcc, [async_fold, {bucket, <<"second_split">>}], S1),
    {ok, Buff2} = ?MODULE:fold_keys(FoldFun, BufferAcc, [{bucket, <<"second_split">>}], S1),

    AsyncKeys2 = lists:flatten([X || {_, X, _, _, _} <- AsyncBuff2()]),
    Keys2  = lists:flatten([X || {_, X, _, _, _} <- Buff2]),
%%    Keys2  = element(2, Buff2),
    ct:pal("AsyncBuff2: ~p~n", [AsyncBuff2()]),
    ct:pal("Buff2: ~p~n", [Buff2]),

    ?assertEqual([<<"k3">>, <<"k4">>, <<"k5">>, <<"k6">>], lists:sort(AsyncKeys2)),
    ?assertEqual([<<"k3">>, <<"k4">>, <<"k5">>, <<"k6">>], lists:sort(Keys2)),


    ok = ?MODULE:special_merge(default, second_split, S1#state{partition = 1}),

    ?MODULE:put(<<"second_split">>, <<"k7">>, [], <<"v1">>, S1#state{partition = 1}),
    ?MODULE:put(<<"second_split">>, <<"k8">>, [], <<"v2">>, S1#state{partition = 1}),

    {async, AsyncBuff3} = ?MODULE:fold_keys(FoldFun, BufferAcc, [async_fold, {bucket, <<"second_split">>}], S1),
    {ok, Buff3} = ?MODULE:fold_keys(FoldFun, BufferAcc, [{bucket, <<"second_split">>}], S1),

    AsyncKeys3 = element(2, AsyncBuff3()),
    Keys3  = element(2, Buff3),

    ?assertEqual([<<"k3">>, <<"k4">>, <<"k5">>, <<"k6">>, <<"k7">>, <<"k8">>], lists:sort(Keys3)),
    ?assertEqual([<<"k3">>, <<"k4">>, <<"k5">>, <<"k6">>, <<"k7">>, <<"k8">>], lists:sort(AsyncKeys3)),

    {ok, _NewRef} = ?MODULE:deactivate_backend(second_split, S1),

    ?MODULE:put(<<"second_split">>, <<"k9">>, [], <<"v1">>, S1),
    ?MODULE:put(<<"second_split">>, <<"k91">>, [], <<"v2">>, S1),

    {async, AsyncBuff4} = ?MODULE:fold_keys(FoldFun, BufferAcc, [async_fold, {bucket, <<"second_split">>}], S1),
    {ok, Buff4} = ?MODULE:fold_keys(FoldFun, BufferAcc, [{bucket, <<"second_split">>}], S1),

    AsyncKeys4 = lists:flatten([X || {_, X, _, _, _} <- AsyncBuff4()]),
    Keys4  = lists:flatten([X || {_, X, _, _, _} <- Buff4]),

    ?assertEqual([<<"k3">>, <<"k4">>, <<"k5">>, <<"k6">>, <<"k7">>, <<"k8">>, <<"k9">>, <<"k91">>], lists:sort(Keys4)),
    ?assertEqual([<<"k3">>, <<"k4">>, <<"k5">>, <<"k6">>, <<"k7">>, <<"k8">>, <<"k9">>, <<"k91">>], lists:sort(AsyncKeys4)),

    ok = ?MODULE:reverse_merge(second_split, default, S1),

    {async, AsyncBuff5} = ?MODULE:fold_keys(FoldFun, BufferAcc, [async_fold, {bucket, <<"second_split">>}], S1),
    {ok, Buff5} = ?MODULE:fold_keys(FoldFun, BufferAcc, [{bucket, <<"second_split">>}], S1),

    AsyncKeys5 = element(2, AsyncBuff5()),
    Keys5  = element(2, Buff5),

    ?assertEqual([<<"k3">>, <<"k4">>, <<"k5">>, <<"k6">>, <<"k7">>, <<"k8">>, <<"k9">>, <<"k91">>], lists:sort(Keys5)),
    ?assertEqual([<<"k3">>, <<"k4">>, <<"k5">>, <<"k6">>, <<"k7">>, <<"k8">>, <<"k9">>, <<"k91">>], lists:sort(AsyncKeys5)),

    ok = stop(S1),
    os:cmd("rm -rf test/bitcask-backend/*").

full_split_test() ->
    ct:pal("########################### FULL_SPLIT_TEST ##################"),
    os:cmd("rm -rf test/bitcask-backend/*"),
    Opts = [{data_root, "test/bitcask-backend"}, {max_file_size, 10}],
    TstampExpire = bitcask_time:tstamp() - 1000,
    {ok, S} = ?MODULE:start(0, Opts),
    {ok, S1} = ?MODULE:start_additional_split({second_split, false}, S),
    FoldFun = fun(_, Key, Acc) -> [Key | Acc] end,

    ?assertEqual(true, check_backend_exists(default, S1)),
    ?assertEqual(true, check_backend_exists(second_split, S1)),
    ?assertEqual(true, is_backend_active(default, S1)),
    ?assertEqual(false, is_backend_active(second_split, S1)),

    ?MODULE:put(<<"b1">>, <<"k1">>, [], <<"v1">>, S1),
    ?MODULE:put(<<"b1">>, <<"k2">>, [], <<"v2">>, S1),
    ?MODULE:put(<<"second_split">>, <<"k1">>, [], <<"v1">>, S1),
    ?MODULE:put(<<"second_split">>, <<"k2">>, [], <<"v2">>, S1),

    {ok, <<"v1">>, _} = ?MODULE:get(<<"b1">>, <<"k1">>, S1),
    {ok, <<"v2">>, _} = ?MODULE:get(<<"b1">>, <<"k2">>, S1),
    {ok, <<"v1">>, _} = ?MODULE:get(<<"second_split">>, <<"k1">>, S1),
    {ok, <<"v2">>, _} = ?MODULE:get(<<"second_split">>, <<"k2">>, S1),

    {ok, Keys1} = ?MODULE:fold_keys(FoldFun, [], [{bucket, <<"b1">>}], S1),
    {ok, Keys2} = ?MODULE:fold_keys(FoldFun, [], [{bucket, <<"second_split">>}], S1),
    ?assertEqual([<<"k1">>, <<"k2">>], lists:sort(Keys1)),
    ?assertEqual([<<"k1">>, <<"k2">>], lists:sort(Keys2)),

    ?MODULE:put(<<"second_split">>, <<"k2">>, [], <<"v22">>, TstampExpire, S1), %% Add expired key to test if it appears after special merge
    {error, not_found, _} = ?MODULE:get(<<"second_split">>, <<"k2">>, S1),

    ?MODULE:activate_backend(second_split, S1),
    ?assertEqual(true, is_backend_active(second_split, S1)),

    ?assertEqual({ok, []}, file:list_dir("test/bitcask-backend/0/second_split")),

    ?MODULE:put(<<"second_split">>, <<"k3">>, [], <<"v3">>, S1#state{partition = 1}),
    ?MODULE:put(<<"second_split">>, <<"k4">>, [], <<"v4">>, S1#state{partition = 1}),

    ?assertNotEqual({ok, []}, file:list_dir("test/bitcask-backend/0/second_split")),

    {ok, <<"v3">>, _} = ?MODULE:get(<<"second_split">>, <<"k3">>, S1#state{partition = 1}),
    {ok, <<"v4">>, _} = ?MODULE:get(<<"second_split">>, <<"k4">>, S1#state{partition = 1}),

    {ok, Keys3} = ?MODULE:fold_keys(FoldFun, [], [{bucket, <<"second_split">>}], S1),
    ?assertEqual([<<"k1">>, <<"k3">>, <<"k4">>], lists:sort(Keys3)),

    ?MODULE:special_merge(default, second_split, S1#state{partition = 1}),
    ?assertNotEqual({ok, []}, file:list_dir("test/bitcask-backend/0/second_split")),

    State = erlang:get(S1#state.ref),
    OpenInstances = element(2, State),
    {second_split, BRef, _, _} = lists:keyfind(second_split, 1, OpenInstances),
    BState = erlang:get(BRef),
    KeyDir = element(10, BState),
    {error, not_found, _} = ?MODULE:get(<<"second_split">>, <<"k2">>, S1#state{partition = 1}),
    not_found = bitcask_nifs:keydir_get(KeyDir, make_bitcask_key(3, {<<"second_split">>, <<"second_split">>}, <<"k2">>)),

    {ok, Keys4} = ?MODULE:fold_keys(FoldFun, [], [{bucket, <<"second_split">>}], S1),
    ?assertEqual([<<"k1">>, <<"k3">>, <<"k4">>], lists:sort(Keys4)),

    ?MODULE:put(<<"second_split">>, <<"k5">>, [], <<"v5">>, S1#state{partition = 1}),
    ?MODULE:put(<<"second_split">>, <<"k6">>, [], <<"v6">>, S1#state{partition = 1}),

    ?MODULE:deactivate_backend(second_split, S1),
    ?assertEqual(false, is_backend_active(second_split, S1)),
    ct:pal("Doing gets #############################"),
    {ok, <<"v1">>, _} = ?MODULE:get(<<"b1">>, <<"k1">>, S1),
    {ok, <<"v2">>, _} = ?MODULE:get(<<"b1">>, <<"k2">>, S1),
    {ok, <<"v1">>, _} = ?MODULE:get(<<"second_split">>, <<"k1">>, S1),
    {ok, <<"v3">>, _} = ?MODULE:get(<<"second_split">>, <<"k3">>, S1),
    {ok, <<"v4">>, _} = ?MODULE:get(<<"second_split">>, <<"k4">>, S1),
    {ok, <<"v5">>, _} = ?MODULE:get(<<"second_split">>, <<"k5">>, S1),
    {ok, <<"v6">>, _} = ?MODULE:get(<<"second_split">>, <<"k6">>, S1),

    {ok, Keys5} = ?MODULE:fold_keys(FoldFun, [], [{bucket, <<"second_split">>}], S1),
    ?assertEqual([<<"k1">>, <<"k3">>, <<"k4">>, <<"k5">>, <<"k6">>], lists:sort(Keys5)),

    ?MODULE:reverse_merge(second_split, default, S1),   %% Don't change partition here so default is returned imitating what MD would return due to inactive split state

    {ok, <<"v1">>, _} = ?MODULE:get(<<"b1">>, <<"k1">>, S1),
    {ok, <<"v2">>, _} = ?MODULE:get(<<"b1">>, <<"k2">>, S1),
    {ok, <<"v1">>, _} = ?MODULE:get(<<"second_split">>, <<"k1">>, S1),
    {ok, <<"v3">>, _} = ?MODULE:get(<<"second_split">>, <<"k3">>, S1),
    {ok, <<"v4">>, _} = ?MODULE:get(<<"second_split">>, <<"k4">>, S1),

    {ok, Keys6} = ?MODULE:fold_keys(FoldFun, [], [{bucket, <<"second_split">>}], S1),
    ?assertEqual([<<"k1">>, <<"k3">>, <<"k4">>, <<"k5">>, <<"k6">>], lists:sort(Keys6)),

    ok = stop(S1),
    os:cmd("rm -rf test/bitcask-backend/*").





-ifdef(EQC).

prop_bitcask_backend() ->
    Path = riak_kv_test_util:get_test_dir("bitcask-backend"),
    ?SETUP(fun() ->
                   application:load(sasl),
                   application:set_env(sasl,
                                        sasl_error_logger,
                                       {file, Path ++ "/riak_kv_bitcask_backend_eqc_sasl.log"}),
                   error_logger:tty(false),
                   error_logger:logfile({open,
                                        Path ++ "/riak_kv_bitcask_backend_eqc.log"}),

                   application:load(bitcask),
                   application:set_env(bitcask, merge_window, never),
                   fun() ->  ?assertCmd("rm -rf " ++ Path ++ "/*") end
           end,
           backend_eqc:prop_backend(?MODULE,
                                    false,
                                    [{data_root, Path}])).
-endif. % EQC

-endif.
