%% -------------------------------------------------------------------
%%
%% riak_console: interface for Riak admin commands
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

%% @doc interface for Riak admin commands

-module(riak_kv_console).

-export([join/1,
         staged_join/1,
         leave/1,
         remove/1,
         status/1,
         vnode_status/1,
         reip/1,
         ringready/1,
         cluster_info/1,
         down/1,
         aae_status/1,
         repair_2i/1,
         reformat_indexes/1,
         reformat_objects/1,
         reload_code/1,
         bucket_type_status/1,
         bucket_type_activate/1,
         bucket_type_create/1,
         bucket_type_update/1,
         bucket_type_reset/1,
         bucket_type_list/1,
         add_split_backend_local/1,
         add_split_backend_local/2,
         activate_split_backend_local/1,
         activate_split_backend_local/2,
         deactivate_split_backend_local/1,
         deactivate_split_backend_local/2,
%%         special_merge/1,
         special_merge_local/1,
         special_merge_local/2,
         reverse_merge_local/1,
         reverse_merge_local/2,
         remove_split_backend_local/1,
         remove_split_backend_local/2
    ]).

-export([ensemble_status/1]).

%% Reused by Yokozuna for printing AAE status.
-export([aae_exchange_status/1,
         aae_repair_status/1,
         aae_tree_status/1]).

-define(ACCEPTED_BACKEND_TYPES, [bucket, key, value]).

join([NodeStr]) ->
    join(NodeStr, fun riak_core:join/1,
         "Sent join request to ~s~n", [NodeStr]).

staged_join([NodeStr]) ->
    Node = list_to_atom(NodeStr),
    join(NodeStr, fun riak_core:staged_join/1,
         "Success: staged join request for ~p to ~p~n", [node(), Node]).

join(NodeStr, JoinFn, SuccessFmt, SuccessArgs) ->
    try
        case JoinFn(NodeStr) of
            ok ->
                io:format(SuccessFmt, SuccessArgs),
                ok;
            {error, not_reachable} ->
                io:format("Node ~s is not reachable!~n", [NodeStr]),
                error;
            {error, different_ring_sizes} ->
                io:format("Failed: ~s has a different ring_creation_size~n",
                          [NodeStr]),
                error;
            {error, unable_to_get_join_ring} ->
                io:format("Failed: Unable to get ring from ~s~n", [NodeStr]),
                error;
            {error, not_single_node} ->
                io:format("Failed: This node is already a member of a "
                          "cluster~n"),
                error;
            {error, self_join} ->
                io:format("Failed: This node cannot join itself in a "
                          "cluster~n"),
                error;
            {error, _} ->
                io:format("Join failed. Try again in a few moments.~n", []),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Join failed ~p:~p", [Exception, Reason]),
            io:format("Join failed, see log for details~n"),
            error
    end.


leave([]) ->
    try
        case riak_core:leave() of
            ok ->
                io:format("Success: ~p will shutdown after handing off "
                          "its data~n", [node()]),
                ok;
            {error, already_leaving} ->
                io:format("~p is already in the process of leaving the "
                          "cluster.~n", [node()]),
                ok;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [node()]),
                error;
            {error, only_member} ->
                io:format("Failed: ~p is the only member.~n", [node()]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Leave failed ~p:~p", [Exception, Reason]),
            io:format("Leave failed, see log for details~n"),
            error
    end.

remove([Node]) ->
    try
        case riak_core:remove(list_to_atom(Node)) of
            ok ->
                io:format("Success: ~p removed from the cluster~n", [Node]),
                ok;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [Node]),
                error;
            {error, only_member} ->
                io:format("Failed: ~p is the only member.~n", [Node]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Remove failed ~p:~p", [Exception, Reason]),
            io:format("Remove failed, see log for details~n"),
            error
    end.

down([Node]) ->
    try
        case riak_core:down(list_to_atom(Node)) of
            ok ->
                io:format("Success: ~p marked as down~n", [Node]),
                ok;
            {error, is_up} ->
                io:format("Failed: ~s is up~n", [Node]),
                error;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [Node]),
                error;
            {error, only_member} ->
                io:format("Failed: ~p is the only member.~n", [Node]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Down failed ~p:~p", [Exception, Reason]),
            io:format("Down failed, see log for details~n"),
            error
    end.

-spec(status([]) -> ok).
status([]) ->
    try
        Stats = riak_kv_status:statistics(),
	StatString = format_stats(Stats,
                    ["-------------------------------------------\n",
		     io_lib:format("1-minute stats for ~p~n",[node()])]),
	io:format("~s\n", [StatString])
    catch
        Exception:Reason ->
            lager:error("Status failed ~p:~p", [Exception,
                    Reason]),
            io:format("Status failed, see log for details~n"),
            error
    end.

-spec(vnode_status([]) -> ok).
vnode_status([]) ->
    try
        case riak_kv_status:vnode_status() of
            [] ->
                io:format("There are no active vnodes.~n");
            Statuses ->
                io:format("~s~n-------------------------------------------~n~n",
                          ["Vnode status information"]),
                print_vnode_statuses(lists:sort(Statuses))
        end
    catch
        Exception:Reason ->
            lager:error("Backend status failed ~p:~p", [Exception,
                    Reason]),
            io:format("Backend status failed, see log for details~n"),
            error
    end.

reip([OldNode, NewNode]) ->
    try
        %% reip is called when node is down (so riak_core_ring_manager is not running),
        %% so it has to use the basic ring operations.
        %%
        %% Do *not* convert to use riak_core_ring_manager:ring_trans.
        %%
        case application:load(riak_core) of
            %% a process, such as cuttlefish, may have already loaded riak_core
            {error,{already_loaded,riak_core}} -> ok;
            ok -> ok
        end,
        RingStateDir = app_helper:get_env(riak_core, ring_state_dir),
        {ok, RingFile} = riak_core_ring_manager:find_latest_ringfile(),
        BackupFN = filename:join([RingStateDir, filename:basename(RingFile)++".BAK"]),
        {ok, _} = file:copy(RingFile, BackupFN),
        io:format("Backed up existing ring file to ~p~n", [BackupFN]),
        Ring = riak_core_ring_manager:read_ringfile(RingFile),
        NewRing = riak_core_ring:rename_node(Ring, OldNode, NewNode),
        ok = riak_core_ring_manager:do_write_ringfile(NewRing),
        io:format("New ring file written to ~p~n",
            [element(2, riak_core_ring_manager:find_latest_ringfile())])
    catch
        Exception:Reason ->
            io:format("Reip failed ~p:~p", [Exception, Reason]),
            error
    end.

%% Check if all nodes in the cluster agree on the partition assignment
-spec(ringready([]) -> ok | error).
ringready([]) ->
    try
        case riak_core_status:ringready() of
            {ok, Nodes} ->
                io:format("TRUE All nodes agree on the ring ~p\n", [Nodes]);
            {error, {different_owners, N1, N2}} ->
                io:format("FALSE Node ~p and ~p list different partition owners\n", [N1, N2]),
                error;
            {error, {nodes_down, Down}} ->
                io:format("FALSE ~p down.  All nodes need to be up to check.\n", [Down]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Ringready failed ~p:~p", [Exception,
                    Reason]),
            io:format("Ringready failed, see log for details~n"),
            error
    end.

cluster_info([OutFile|Rest]) ->
    try
        case lists:reverse(atomify_nodestrs(Rest)) of
            [] ->
                cluster_info:dump_all_connected(OutFile);
            Nodes ->
                cluster_info:dump_nodes(Nodes, OutFile)
        end
    catch
        error:{badmatch, {error, eacces}} ->
            io:format("Cluster_info failed, permission denied writing to ~p~n", [OutFile]);
        error:{badmatch, {error, enoent}} ->
            io:format("Cluster_info failed, no such directory ~p~n", [filename:dirname(OutFile)]);
        error:{badmatch, {error, enotdir}} ->
            io:format("Cluster_info failed, not a directory ~p~n", [filename:dirname(OutFile)]);
        Exception:Reason ->
            lager:error("Cluster_info failed ~p:~p",
                [Exception, Reason]),
            io:format("Cluster_info failed, see log for details~n"),
            error
    end.

reload_code([]) ->
    case app_helper:get_env(riak_kv, add_paths) of
        List when is_list(List) ->
            _ = [ reload_path(filename:absname(Path)) || Path <- List ],
            ok;
        _ -> ok
    end.

reload_path(Path) ->
    {ok, Beams} = file:list_dir(Path),
    [ reload_file(filename:absname(Beam, Path)) || Beam <- Beams, ".beam" == filename:extension(Beam) ].

reload_file(Filename) ->
    Mod = list_to_atom(filename:basename(Filename, ".beam")),
    case code:is_loaded(Mod) of
        {file, Filename} ->
            code:soft_purge(Mod),
            {module, Mod} = code:load_file(Mod),
            io:format("Reloaded module ~w from ~s.~n", [Mod, Filename]);
        {file, Other} ->
            io:format("CONFLICT: Module ~w originally loaded from ~s, won't reload from ~s.~n", [Mod, Other, Filename]);
        _ ->
            io:format("Module ~w not yet loaded, skipped.~n", [Mod])
    end.

aae_status([]) ->
    ExchangeInfo = riak_kv_entropy_info:compute_exchange_info(),
    aae_exchange_status(ExchangeInfo),
    io:format("~n"),
    TreeInfo = riak_kv_entropy_info:compute_tree_info(),
    aae_tree_status(TreeInfo),
    io:format("~n"),
    aae_repair_status(ExchangeInfo).

aae_exchange_status(ExchangeInfo) ->
    io:format("~s~n", [string:centre(" Exchanges ", 79, $=)]),
    io:format("~-49s  ~-12s  ~-12s~n", ["Index", "Last (ago)", "All (ago)"]),
    io:format("~79..-s~n", [""]),
    _ = [begin
         Now = os:timestamp(),
         LastStr = format_timestamp(Now, LastTS),
         AllStr = format_timestamp(Now, AllTS),
         io:format("~-49b  ~-12s  ~-12s~n", [Index, LastStr, AllStr]),
         ok
     end || {Index, LastTS, AllTS, _Repairs} <- ExchangeInfo],
    ok.

aae_repair_status(ExchangeInfo) ->
    io:format("~s~n", [string:centre(" Keys Repaired ", 79, $=)]),
    io:format("~-49s  ~s  ~s  ~s~n", ["Index",
                                      string:centre("Last", 8),
                                      string:centre("Mean", 8),
                                      string:centre("Max", 8)]),
    io:format("~79..-s~n", [""]),
    _ = [begin
         io:format("~-49b  ~s  ~s  ~s~n", [Index,
                                           string:centre(integer_to_list(Last), 8),
                                           string:centre(integer_to_list(Mean), 8),
                                           string:centre(integer_to_list(Max), 8)]),
         ok
     end || {Index, _, _, {Last,_Min,Max,Mean}} <- ExchangeInfo],
    ok.

aae_tree_status(TreeInfo) ->
    io:format("~s~n", [string:centre(" Entropy Trees ", 79, $=)]),
    io:format("~-49s  Built (ago)~n", ["Index"]),
    io:format("~79..-s~n", [""]),
    _ = [begin
         Now = os:timestamp(),
         BuiltStr = format_timestamp(Now, BuiltTS),
         io:format("~-49b  ~s~n", [Index, BuiltStr]),
         ok
     end || {Index, BuiltTS} <- TreeInfo],
    ok.

format_timestamp(_Now, undefined) ->
    "--";
format_timestamp(Now, TS) ->
    riak_core_format:human_time_fmt("~.1f", timer:now_diff(Now, TS)).

parse_int(IntStr) ->
    try
        list_to_integer(IntStr)
    catch
        error:badarg ->
            undefined
    end.

index_reformat_options([], Opts) ->
    Defaults = [{concurrency, 2}, {batch_size, 100}],
    AddIfAbsent =
        fun({Name,Val}, Acc) ->
            case lists:keymember(Name, 1, Acc) of
                true ->
                    Acc;
                false ->
                    [{Name, Val} | Acc]
            end
        end,
    lists:foldl(AddIfAbsent, Opts, Defaults);
index_reformat_options(["--downgrade"], Opts) ->
    [{downgrade, true} | Opts];
index_reformat_options(["--downgrade" | More], _Opts) ->
    io:format("Invalid arguments after downgrade switch : ~p~n", [More]),
    undefined;
index_reformat_options([IntStr | Rest], Opts) ->
    HasConcurrency = lists:keymember(concurrency, 1, Opts),
    HasBatchSize = lists:keymember(batch_size, 1, Opts),
    case {parse_int(IntStr), HasConcurrency, HasBatchSize} of
        {_, true, true} ->
            io:format("Expected --downgrade instead of ~p~n", [IntStr]),
            undefined;
        {undefined, _, _ } ->
            io:format("Expected integer parameter instead of ~p~n", [IntStr]),
            undefined;
        {IntVal, false, false} ->
            index_reformat_options(Rest, [{concurrency, IntVal} | Opts]);
        {IntVal, true, false} ->
            index_reformat_options(Rest, [{batch_size, IntVal} | Opts])
    end;
index_reformat_options(_, _) ->
    undefined.

reformat_indexes(Args) ->
    Opts = index_reformat_options(Args, []),
    case Opts of
        undefined ->
            io:format("Expected options: <concurrency> <batch size> [--downgrade]~n"),
            ok;
        _ ->
            start_reformat(riak_kv_util, fix_incorrect_index_entries, [Opts]),
            io:format("index reformat started with options ~p ~n", [Opts]),
            io:format("check console.log for status information~n"),
            ok
    end.

reformat_objects([KillHandoffsStr]) ->
    reformat_objects([KillHandoffsStr, "2"]);
reformat_objects(["true", ConcurrencyStr | _Rest]) ->
    reformat_objects([true, ConcurrencyStr]);
reformat_objects(["false", ConcurrencyStr | _Rest]) ->
    reformat_objects([false, ConcurrencyStr]);
reformat_objects([KillHandoffs, ConcurrencyStr]) when is_atom(KillHandoffs) ->
    case parse_int(ConcurrencyStr) of
        C when C > 0 ->
            start_reformat(riak_kv_reformat, run,
                           [v0, [{concurrency, C}, {kill_handoffs, KillHandoffs}]]),
            io:format("object reformat started with concurrency ~p~n", [C]),
            io:format("check console.log for status information~n");
        _ ->
            io:format("ERROR: second argument must be an integer greater than zero.~n"),
            error

    end;
reformat_objects(_) ->
    io:format("ERROR: first argument must be either \"true\" or \"false\".~n"),
    error.

start_reformat(M, F, A) ->
    spawn(fun() -> run_reformat(M, F, A) end).

run_reformat(M, F, A) ->
    try erlang:apply(M, F, A)
    catch
        Err:Reason ->
            lager:error("index reformat crashed with error type ~p and reason: ~p",
                        [Err, Reason])
    end.

bucket_type_status([TypeStr]) ->
    Type = unicode:characters_to_binary(TypeStr, utf8, utf8),
    Return = bucket_type_print_status(Type, riak_core_bucket_type:status(Type)),
    bucket_type_print_props(bucket_type_raw_props(Type)),
    Return.


bucket_type_raw_props(<<"default">>) ->
    riak_core_bucket_props:defaults();
bucket_type_raw_props(Type) ->
    riak_core_claimant:get_bucket_type(Type, undefined, false).

bucket_type_print_status(Type, undefined) ->
    io:format("~ts is not an existing bucket type~n", [Type]),
    {error, undefined};
bucket_type_print_status(Type, created) ->
    io:format("~ts has been created but cannot be activated yet~n", [Type]),
    {error, not_ready};
bucket_type_print_status(Type, ready) ->
    io:format("~ts has been created and may be activated~n", [Type]),
    ok;
bucket_type_print_status(Type, active) ->
    io:format("~ts is active~n", [Type]),
    ok.

bucket_type_print_props(undefined) ->
    ok;
bucket_type_print_props(Props) ->
    io:format("~n"),
    [io:format("~p: ~p~n", [K, V]) || {K, V} <- Props].

bucket_type_activate([TypeStr]) ->
    Type = unicode:characters_to_binary(TypeStr, utf8, utf8),
    IsFirst = bucket_type_is_first(),
    bucket_type_print_activate_result(Type, riak_core_bucket_type:activate(Type), IsFirst).

bucket_type_print_activate_result(Type, ok, IsFirst) ->
    io:format("~ts has been activated~n", [Type]),
    case IsFirst of
        true ->
            io:format("~n"),
            io:format("WARNING: Nodes in this cluster can no longer be~n"
                      "downgraded to a version of Riak prior to 2.0~n");
        false ->
            ok
    end;
bucket_type_print_activate_result(Type, {error, undefined}, _IsFirst) ->
    bucket_type_print_status(Type, undefined);
bucket_type_print_activate_result(Type, {error, not_ready}, _IsFirst) ->
    bucket_type_print_status(Type, created).

bucket_type_create([TypeStr, ""]) ->
    Type = unicode:characters_to_binary(TypeStr, utf8, utf8),
    EmptyProps = {struct, [{<<"props">>, {struct, []}}]},
    bucket_type_create(Type, EmptyProps);
bucket_type_create([TypeStr, PropsStr]) ->
    Type = unicode:characters_to_binary(TypeStr, utf8, utf8),
    bucket_type_create(Type, catch mochijson2:decode(PropsStr)).

bucket_type_create(Type, {struct, Fields}) ->
    case proplists:get_value(<<"props">>, Fields) of
        {struct, Props} ->
            ErlProps = [riak_kv_wm_utils:erlify_bucket_prop(P) || P <- Props],
            bucket_type_print_create_result(Type, riak_core_bucket_type:create(Type, ErlProps));
        _ ->
            io:format("Cannot create bucket type ~ts: no props field found in json~n", [Type]),
            error
    end;
bucket_type_create(Type, _) ->
    io:format("Cannot create bucket type ~ts: invalid json~n", [Type]),
    error.

bucket_type_print_create_result(Type, ok) ->
    io:format("~ts created~n", [Type]),
    case bucket_type_is_first() of
        true ->
            io:format("~n"),
            io:format("WARNING: After activating ~ts, nodes in this cluster~n"
                      "can no longer be downgraded to a version of Riak "
                      "prior to 2.0~n", [Type]);
        false ->
            ok
    end;
bucket_type_print_create_result(Type, {error, Reason}) ->
    io:format("Error creating bucket type ~ts:~n", [Type]),
    io:format(bucket_error_xlate(Reason)),
    io:format("~n"),
    error.

bucket_type_update([TypeStr, PropsStr]) ->
    Type = unicode:characters_to_binary(TypeStr, utf8, utf8),
    bucket_type_update(Type, catch mochijson2:decode(PropsStr)).

bucket_type_update(Type, {struct, Fields}) ->
    case proplists:get_value(<<"props">>, Fields) of
        {struct, Props} ->
            ErlProps = [riak_kv_wm_utils:erlify_bucket_prop(P) || P <- Props],
            bucket_type_print_update_result(Type, riak_core_bucket_type:update(Type, ErlProps));
        _ ->
            io:format("Cannot create bucket type ~ts: no props field found in json~n", [Type]),
            error
    end;
bucket_type_update(Type, _) ->
    io:format("Cannot update bucket type: ~ts: invalid json~n", [Type]),
    error.

bucket_type_print_update_result(Type, ok) ->
    io:format("~ts updated~n", [Type]);
bucket_type_print_update_result(Type, {error, Reason}) ->
    io:format("Error updating bucket type ~ts:~n", [Type]),
    io:format(bucket_error_xlate(Reason)),
    io:format("~n"),
    error.

bucket_type_reset([TypeStr]) ->
    Type = unicode:characters_to_binary(TypeStr, utf8, utf8),
    bucket_type_print_reset_result(Type, riak_core_bucket_type:reset(Type)).

bucket_type_print_reset_result(Type, ok) ->
    io:format("~ts reset~n", [Type]);
bucket_type_print_reset_result(Type, {error, Reason}) ->
    io:format("Error updating bucket type ~ts: ~p~n", [Type, Reason]),
    error.

bucket_type_list([]) ->
    It = riak_core_bucket_type:iterator(),
    io:format("default (active)~n"),
    bucket_type_print_list(It).

bucket_type_print_list(It) ->
    case riak_core_bucket_type:itr_done(It) of
        true ->
            riak_core_bucket_type:itr_close(It);
        false ->
            {Type, Props} = riak_core_bucket_type:itr_value(It),
            ActiveStr = case proplists:get_value(active, Props, false) of
                            true -> "active";
                            false -> "not active"
                        end,

            io:format("~ts (~s)~n", [Type, ActiveStr]),
            bucket_type_print_list(riak_core_bucket_type:itr_next(It))
    end.

bucket_type_is_first() ->
    It = riak_core_bucket_type:iterator(),
    bucket_type_is_first(It, false).

bucket_type_is_first(It, true) ->
    %% found an active bucket type
    riak_core_bucket_type:itr_close(It),
    false;
bucket_type_is_first(It, false) ->
    case riak_core_bucket_type:itr_done(It) of
        true ->
            %% no active bucket types found
            ok = riak_core_bucket_type:itr_close(It),
            true;
        false ->
            {_, Props} = riak_core_bucket_type:itr_value(It),
            Active = proplists:get_value(active, Props, false),
            bucket_type_is_first(riak_core_bucket_type:itr_next(It), Active)
    end.

%%add_split_backend([Name]) when is_list(Name) ->
%%    add_split_backend([list_to_atom(Name)]);
%%add_split_backend([Name]) ->
%%    put_and_confirm_metadata(Name).

%% TODO create equivilant functions that work across the cluster rather than locally. One way would be via event handler previously
%% however each node as it receives notification matches with the node name passed via the sender to not put on the same node.
%% Or we manually call these on each node instead.
%% Metadata splits has 3 stages of value, 1st is `false` 2nd `active` and 3rd `special_merge` each value
%% representing the command that has been triggered on the that split.
add_split_backend_local([Name]) when is_list(Name) ->
    add_split_backend_local(list_to_atom(Name));
add_split_backend_local([Name, Partition]) when is_list(Name) andalso is_list(Partition) ->
    add_split_backend_local(list_to_atom(Name), list_to_integer(Partition));
add_split_backend_local(Name) ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    Partitions = riak_core_ring:my_indices(R),
    case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
        undefined ->
            [riak_kv_vnode:add_split_backend(Name, Partition) || Partition <- Partitions],
            case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
                undefined ->
                    io:format("Failed to create backend: ~p on all Partitions~n", [Name]),
                    ok;
                X when length(X) =:= length(Partitions) ->
                    io:format("Succesfully created backend: ~p~n", [Name]),
                    ok
            end;
        Statuses ->
            case [Partition || Partition <- Partitions, false =:= lists:keymember(Partition, 1, Statuses)] of
                [] ->
                    io:format("Backend: ~p has already been created on all partitions for this node~n", [Name]),
                    ok;
                NewParts ->
                    [riak_kv_vnode:add_split_backend(Name, Partition) || Partition <- NewParts],
                    case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
                        undefined ->
                            io:format("Failed to create backend: ~p on select Partitions~n", [Name]),
                            error;
                        X when length(X) =:= length(Partitions) ->
                            io:format("Succesfully created backend: ~p~n", [Name]),
                            ok
                    end
            end
    end.

add_split_backend_local(Name, Partition) ->
    case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
        undefined ->
            riak_kv_vnode:add_split_backend(Name, Partition),
            case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
                undefined ->
                    io:format("Failed to create backend: ~p on Partition: ~p~n", [Name, Partition]),
                    ok;
                _ ->
                    io:format("Succesfully created backend: ~p on Partition: ~p~n", [Name, Partition]),
                    ok
            end;
        Statuses ->
            case lists:keyfind(Partition, 1, Statuses) of
                false ->
                    riak_kv_vnode:add_split_backend(Name, Partition),
                    case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
                        undefined ->
                            io:format("Failed to create backend: ~p on Partition: ~p~n", [Name, Partition]),
                            ok;
                        _ ->
                            io:format("Succesfully created backend: ~p on Partition: ~p~n", [Name, Partition]),
                            ok
                    end;
                {Partition, _State} ->
                    io:format("Backend: ~p already exists on Partition: ~p~n", [Name, Partition]),
                    error
            end
    end.

%%activate_split_backend([Name]) when is_list(Name) ->
%%    activate_split_backend(list_to_atom(Name));
%%activate_split_backend(Name) when is_atom(Name) ->
%%    case riak_core_metadata:get({split_backend, splits}, Name) of
%%        false ->
%%            riak_core_metadata:put({split_backend, splits}, Name, active),
%%            case riak_core_metadata:get({split_backend, splits}, Name) of
%%                false ->
%%                    io:format("Failed to activate backend: ~p Please investigate", [Name]),
%%                    error;
%%                active ->
%%                    io:format("Succesfully activated backend: ~p~n", [Name]),
%%                    ok
%%            end;
%%        active ->
%%            io:format("Backend: ~p is already active", [Name]),
%%            ok;
%%        undefined ->
%%            io:format("Backend: ~p does not exist so cannot be activated~n", [Name]),
%%            error
%%    end.

activate_split_backend_local([Name]) when is_list(Name) ->
    activate_split_backend_local(list_to_atom(Name));
activate_split_backend_local([Name, Partition]) when is_list(Name) andalso is_list(Partition) ->
    activate_split_backend_local(list_to_atom(Name), list_to_integer(Partition));
activate_split_backend_local(Name) when is_atom(Name) ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    Partitions = riak_core_ring:my_indices(R),
    PLength = length(Partitions),
    case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
        undefined ->
            io:format("Backend: ~p does not exist for this node so cannot be activated~n", [Name]),
            error;
        BackendStatus when length(BackendStatus) =:= PLength ->
            Responses = lists:foldl(
                fun(Partition, Acc) ->
                    case riak_kv_vnode:activate_split_backend(Name, Partition) of
                        ok ->
                            [ok | Acc];
                        _ ->
                            Acc
                    end
                end, [], Partitions),
            RLength = length(Responses),
            case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
                undefined ->
                    io:format("Backend: ~p attampted to be activated but is no longer in metadata please investigate~n", [Name]),
                    error;
                States when length(States) =:= RLength ->
                    ActiveStates = [{Partition, State} || {Partition, State} <- States, State =:= active],
                    case length(ActiveStates) of
                        RLength ->
                            io:format("Backend: ~p has been succesfully activated on each partition of this node~n", [Name]),
                            ok;
                        _ ->
                            io:format("Backend: ~p could not be activated on every partition, please investigate~n", [Name]),
                            error
                    end;
                _ ->
                    io:format("There is a discrepancy between the number of partition backends in metadata and succesful responses from activating partitions, please investigate. Backend: ~p", [Name]),
                    error
            end;
        _ -> %% TODO Review if we should still go ahead and activate those that do exist or wait for all partitions to be in sync and have the backend
            io:format("Backend: ~p has not been added on every partition for this node so cannot be activated for all partitions~n", [Name]),
            error
    end.

activate_split_backend_local(Name, Partition) when is_atom(Name) ->
    case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
        undefined ->
            io:format("Backend: ~p does not exist on this node so cannot be activated~n", [Name]),
            error;
        BackendStatus ->
            case lists:keyfind(Partition, 1, BackendStatus) of
                false ->
                    io:format("Backend: ~p does not exist  on Partition: ~p so cannot be activated~n", [Name, Partition]),
                    error;
                {Partition, active} ->
                    io:format("Backend: ~p is already active", [Name]),
                    ok;
                {Partition, special_merge} ->
                    io:format("Backend ~p has been special merged meaning it is already active", [Name]),
                    ok;
                {Partition, false} ->
                    ok = riak_kv_vnode:activate_split_backend(Name, Partition),
                    BackendStatus1 = riak_core_metadata:get({split_backend, splits}, {Name, node()}),
                    case lists:keyfind(Partition, 1, BackendStatus1) of
                        false ->
                            io:format("Failed to activate backend: ~p Please investigate", [Name]),
                            error;
                        {Partition, active} ->
                            io:format("Succesfully activated backend: ~p~n", [Name]),
                            ok;
                        _ ->
                            io:format("Backend is in wrong state after activation, investigate what has happened."),
                            ok
                    end
            end
    end.

deactivate_split_backend_local([Name]) when is_list(Name) ->
    deactivate_split_backend_local(list_to_atom(Name));
deactivate_split_backend_local([Name, Partition]) when is_list(Name) andalso is_list(Partition) ->
    deactivate_split_backend_local(list_to_atom(Name), list_to_integer(Partition));
deactivate_split_backend_local(Name) when is_atom(Name) ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    Partitions = riak_core_ring:my_indices(R),
    PLength = length(Partitions),
    case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
        undefined ->
            io:format("Backend: ~p does not exist for this node so cannot be deactivated~n", [Name]),
            error;
        BackendStatus when length(BackendStatus) =:= PLength ->
            Responses = lists:foldl(
                fun(Partition, Acc) ->
                    case riak_kv_vnode:deactivate_split_backend(Name, Partition) of
                        ok ->
                            [ok | Acc];
                        _ ->
                            Acc
                    end
                end, [], Partitions),
            RLength = length(Responses),
            case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
                undefined ->
                    io:format("Backend: ~p attampted to be deactivated but is no longer in metadata please investigate~n", [Name]),
                    error;
                States when length(States) =:= RLength ->
                    ActiveStates = [{Partition, State} || {Partition, State} <- States, State =:= false],
                    case length(ActiveStates) of
                        RLength ->
                            io:format("Backend: ~p has been succesfully deactivated on each partition of this node~n", [Name]),
                            ok;
                        _ ->
                            io:format("Backend: ~p could not be deactivated on every partition, please investigate~n", [Name]),
                            error
                    end;
                _ ->
                    io:format("There is a discrepancy between the number of partition backends in metadata and succesful responses from deactivating partitions, please investigate. Backend: ~p", [Name]),
                    error
            end;
        _ -> %% TODO Review if we should still go ahead and activate those that do exist or wait for all partitions to be in sync and have the backend
            io:format("Backend: ~p has not been added on every partition for this node so cannot be activated for all partitions~n", [Name]),
            error
    end.

deactivate_split_backend_local(Name, Partition) when is_atom(Name) ->
    case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
        undefined ->
            io:format("Backend: ~p does not exist on this node so cannot be deactivated~n", [Name]),
            error;
        BackendStatus ->
            case lists:keyfind(Partition, 1, BackendStatus) of
                false ->
                    io:format("Backend: ~p does not exist  on Partition: ~p so cannot be deactivated~n", [Name, Partition]),
                    error;
                {Partition, false} ->
                    io:format("Backend: ~p is already active", [Name]),
                    ok;
                _ -> %% Can be deactivated if active or special_merge state
                    ok = riak_kv_vnode:deactivate_split_backend(Name, Partition),
                    BackendStatus1 = riak_core_metadata:get({split_backend, splits}, {Name, node()}),
                    case lists:keyfind(Partition, 1, BackendStatus1) of
                        false ->
                            io:format("Failed to deactivate backend: ~p It is no longer in metadata please investigate", [Name]),
                            error;
                        {Partition, false} ->
                            io:format("Succesfully deactivated backend: ~p~n", [Name]),
                            ok;
                        _ ->
                            io:format("Backend is in wrong state after activation, investigate what has happened."),
                            ok
                    end
            end
    end.

%% Metadata splits has 3 stages of value, 1st is `false` 2nd `active` and 3rd `special_merge` each value
%% representing the command that has been triggered on the that split.
special_merge_local([Name]) when is_list(Name) ->
    special_merge_local(list_to_atom(Name));
special_merge_local([Name, Partition]) when is_list(Name) andalso is_list(Partition) ->
    special_merge_local(list_to_atom(Name), list_to_integer(Partition));
special_merge_local(Name) ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    Partitions = riak_core_ring:my_indices(R),
    case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
        undefined ->
            io:format("Backend: ~p does not exist so cannot be special_merged~n", [Name]),
            error;
        BackendStatus when length(BackendStatus) =:= length(Partitions) ->
            Responses = lists:foldl(
                fun(Partition, Acc) ->
                    case riak_kv_vnode:special_merge(Name, Partition) of
                        ok ->
                            [ok | Acc];
                        _ ->
                            Acc
                    end
                end, [], Partitions),
            RLength = length(Responses),
            case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
                undefined ->
                    io:format("Backend: ~p attempted to be special_merge but is no longer in metadata please investigate~n", [Name]),
                    error;
                BackendStatus1 when length(BackendStatus1) =:= RLength ->
                    ActiveStates = [{Partition, State} || {Partition, State} <- BackendStatus1, State =:= special_merge],
                    case length(ActiveStates) of
                        RLength ->
                            io:format("Backend: ~p has been succesfully special_merge on each partition of this node~n", [Name]),
                            ok;
                        _ ->
                            io:format("Backend: ~p could not be special_merge on every partition, please investigate~n", [Name]),
                            error
                    end;
                _ ->
                    io:format("There is a discrepancy between the number of partition backends in metadata and succesful responses from special_merge partitions, please investigate. Backend: ~p", [Name]),
                    error
            end;
        _ -> %% TODO Review if we should still go ahead and activate those that do exist or wait for all partitions to be in sync and have the backend
            io:format("Backend: ~p has not been added on every partition for this node so cannot be activated for all partitions~n", [Name]),
            error
    end.

special_merge_local(Name, Partition) ->
    case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
        undefined ->
            io:format("Backend: ~p does not exist on this node so cannot be special merged~n", [Name]),
            error;
        BackendStatus ->
            case lists:keyfind(Partition, 1, BackendStatus) of
                false ->
                    io:format("Backend: ~p does not exist  on Partition: ~p so cannot be special_merged~n", [Name, Partition]),
                    error;
                {Partition, false} ->
                    io:format("Backend: ~p has not yet been activated on Partition: ~p so cannot be special_merged", [Name, Partition]),
                    ok;
                {Partition, special_merge} ->
                    io:format("Backend ~p has already been special_merged on this Partition: ~p~n", [Name, Partition]),
                    ok;
                {Partition, active} ->
                    ok = riak_kv_vnode:special_merge(Name, Partition),
                    BackendStatus1 = riak_core_metadata:get({split_backend, splits}, {Name, node()}),
                    case lists:keyfind(Partition, 1, BackendStatus1) of
                        false ->
                            io:format("Failed to special_merge backend: ~p Please investigate", [Name]),
                            error;
                        {Partition, special_merge} ->
                            io:format("Succesfully special_merged backend: ~p on this Partition: ~p~n", [Name, Partition]),
                            ok;
                        _ ->
                            io:format("Backend is in wrong state after activation, investigate what has happened.~n"),
                            ok
                    end
            end
    end.

%%    case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
%%        active ->
%%            riak_kv_vnode:special_merge(Name, Partition),
%%            case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
%%                special_merge ->
%%                    io:format("Succesfully merged backend: ~p~n", [Name]),
%%                    ok;
%%                _ ->
%%                    io:format("Failed to activate backend: ~p Please investigate", [Name]),
%%                    error
%%            end;
%%        false ->
%%            io:format("Cannot special merge Bucket: ~p as it is not yet active", [Name]),
%%            ok;
%%        special_merge ->
%%            io:format("Backend has already been special merged"),
%%            ok;
%%        undefined ->
%%            io:format("Backend: ~p does not exist so cannot be activated~n", [Name]),
%%            error
%%    end.

%% Metadata splits has 3 stages of value, 1st is `false` 2nd `active` and 3rd `special_merge` each value
%% representing the command that has been triggered on the that split.
%%special_merge([Name]) when is_list(Name) ->
%%    special_merge([list_to_atom(Name)]);
%%special_merge([Name]) ->
%%    case riak_core_metadata:get({split_backend, splits}, Name) of
%%        active ->
%%            riak_core_metadata:put({split_backend, splits}, Name, special_merge),
%%            case riak_core_metadata:get({split_backend, splits}, Name) of
%%                special_merge ->
%%                    io:format("Succesfully merged backend: ~p~n", [Name]),
%%                    ok;
%%                _ ->
%%                    io:format("Failed to activate backend: ~p Please investigate", [Name]),
%%                    error
%%            end;
%%        false ->
%%            io:format("Cannot special merge Bucket: ~p as it is not yet active", [Name]),
%%            ok;
%%        special_merge ->
%%            io:format("Backend has already been special merged"),
%%            ok;
%%        undefined ->
%%            io:format("Backend: ~p does not exist so cannot be activated~n", [Name]),
%%            error
%%    end.

%% Metadata splits has 3 stages of value, 1st is `false` 2nd `active` and 3rd `special_merge` each value
%% representing the command that has been triggered on the that split.
reverse_merge_local([Name]) when is_list(Name) ->
    reverse_merge_local(list_to_atom(Name));
reverse_merge_local([Name, Partition]) when is_list(Name) andalso is_list(Partition) ->
    reverse_merge_local(list_to_atom(Name), list_to_integer(Partition));
reverse_merge_local(Name) ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    Partitions = riak_core_ring:my_indices(R),
    case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
        undefined ->
            io:format("Backend: ~p does not exist so cannot be reverse_merged~n", [Name]),
            error;
        BackendStatus when length(BackendStatus) =:= length(Partitions) ->
%%            _Responses = lists:foldl(
%%                fun(Partition, Acc) ->
%%                    case riak_kv_vnode:reverse_merge(Name, Partition) of
%%                        ok ->
%%                            [ok | Acc];
%%                        _ ->
%%                            Acc
%%                    end
%%                end, [], Partitions),
            [riak_kv_vnode:reverse_merge(Name, Partition) || Partition <- Partitions],
            case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
                undefined ->
                    io:format("Backend: ~p was succesfully reverse merged on all partitions and removed as a split backend~n", [Name]),
                    ok;
                _ ->
                    io:format("The backend: ~p still exists in metadata when it should have been removed. It may still exist in bitcask, please investigate", [Name]),
                    error
            end
    end.

reverse_merge_local(Name, Partition) ->
    case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
        undefined ->
            io:format("Backend: ~p does not exist on this node so cannot be reverse_merged~n", [Name]),
            error;
        BackendStatus ->
            case lists:keyfind(Partition, 1, BackendStatus) of
                false ->
                    io:format("Backend: ~p does not exist on Partition: ~p so cannot be reverse_merged~n", [Name, Partition]),
                    error;
                {Partition, special_merge} ->
                    io:format("Backend ~p  on partition: ~p state is 'special_merge' it must be deactivated before it can be reverse merged", [Name, Partition]),
                    ok;
                {Partition, active} ->
                    io:format("Backend ~p  on partition: ~p state is 'active' it must be deactivated before it can be reverse merged", [Name, Partition]),
                    ok;
                {Partition, false} ->
                    ok = riak_kv_vnode:reverse_merge(Name, Partition),
                    BackendStatus1 = riak_core_metadata:get({split_backend, splits}, {Name, node()}),
                    case lists:keyfind(Partition, 1, BackendStatus1) of
                        {Partition, special_merge} ->
                            io:format("Failed to reverse_merge backend: ~p  its still in 'special_merge' state. Please investigate", [Name]),
                            error;
                        false ->
                            io:format("Succesfully reverse_merged backend: ~p on this Partition: ~p~n", [Name, Partition]),
                            ok;
                        _ ->
                            io:format("Backend is in wrong state after activation, investigate what has happened.~n"),
                            error
                    end
            end
    end.

remove_split_backend_local([Name]) when is_list(Name) ->
    remove_split_backend_local(list_to_atom(Name));
remove_split_backend_local([Name, Partition]) when is_list(Name) andalso is_list(Partition) ->
    remove_split_backend_local(list_to_atom(Name), list_to_integer(Partition));
remove_split_backend_local(Name) ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    Partitions = riak_core_ring:my_indices(R),
    case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
        undefined ->
            io:format("Backend ~p does not exist so cannot be removed.", [Name]),
            ok;
        BackendStatus when length(BackendStatus) =:= length(Partitions) ->
            Responses = lists:foldl(
                fun(Partition, Acc) ->
                    case riak_kv_vnode:remove_split_backend(Name, Partition) of
                        ok ->
                            [ok | Acc];
                        _ ->
                            Acc
                    end
                end, [], Partitions),
            _RLength = length(Responses),
            case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
                undefined ->
                    io:format("Backend: ~p has been succesfully removed from all partitions on this node~n", [Name]),
                    ok;
%%                BackendStatus1 when length(BackendStatus1) =:= RLength ->
%%                    ActiveStates = [{Partition, State} || {Partition, State} <- BackendStatus1, State =:= special_merge],
%%                    case length(ActiveStates) of
%%                        RLength ->
%%                            io:format("Backend: ~p has been succesfully special_merge on each partition of this node~n", [Name]),
%%                            ok;
%%                        _ ->
%%                            io:format("Backend: ~p could not be special_merge on every partition, please investigate~n", [Name]),
%%                            error
%%                    end;
                _ ->
                    io:format("Backend ~p is still present in metadata for some or all partitions. Please investigate", [Name]),
                    error
            end;
        _ -> %% TODO Review if we should still go ahead and activate those that do exist or wait for all partitions to be in sync and have the backend
            io:format("Backend: ~p has not been added on every partition for this node so cannot be removed from all partitions~n", [Name]),
            error
    end.

remove_split_backend_local(Name, Partition) ->
    case riak_core_metadata:get({split_backend, splits}, {Name, node()}) of
        undefined ->
            io:format("Backend: ~p does not exist on this node so cannot be removed~n", [Name]),
            error;
        BackendStatus ->
            case lists:keyfind(Partition, 1, BackendStatus) of
                false ->
                    io:format("Backend: ~p does not exist on Partition: ~p so cannot be removed~n", [Name, Partition]),
                    error;
                {Partition, _} ->
                    ok = riak_kv_vnode:remove_split_backend(Name, Partition),
                    BackendStatus1 = riak_core_metadata:get({split_backend, splits}, {Name, node()}),
                    case lists:keyfind(Partition, 1, BackendStatus1) of
                        false ->
                            io:format("Succesfully removed backend: ~p for partitio: ~p~n", [Name, Partition]),
                            ok;
%%                        {Partition, special_merge} ->
%%                            io:format("Succesfully special_merged backend: ~p on this Partition: ~p~n", [Name, Partition]),
%%                            ok;
                        _ ->
                            io:format("Backend ~p still present in metadata for partition: ~p please investigate what has happened.~n", [Name, Partition]),
                            ok
                    end
%%                {Partition, false} ->
%%                    io:format("Backend: ~p has not yet been activated on Partition: ~p so cannot be special_merged", [Name, Partition]),
%%                    ok;
%%                {Partition, special_merge} ->
%%                    io:format("Backend ~p has already been special_merged on this Partition: ~p~n", [Name, Partition]),
%%                    ok;
%%                {Partition, active} ->
%%                    ok = riak_kv_vnode:special_merge(Name, Partition),
%%                    BackendStatus1 = riak_core_metadata:get({split_backend, splits}, {Name, node()}),
%%                    case lists:keyfind(Partition, 1, BackendStatus1) of
%%                        false ->
%%                            io:format("Failed to special_merge backend: ~p Please investigate", [Name]),
%%                            error;
%%                        {Partition, special_merge} ->
%%                            io:format("Succesfully special_merged backend: ~p on this Partition: ~p~n", [Name, Partition]),
%%                            ok;
%%                        _ ->
%%                            io:format("Backend is in wrong state after activation, investigate what has happened.~n"),
%%                            ok
%%                    end
            end
    end.


%%remove_split_backend([Type, Name]) when is_list(Type) andalso is_list(Name) ->
%%    remove_split_backend([list_to_atom(Type), list_to_atom(Name)]);
%%remove_split_backend([Type, Name]) ->
%%    case riak_core_metadata:get({split_backend, splits}, Name) of
%%        undefined ->
%%            io:format("Attempting to remove backend ~p but it does not exist in metadata.", [{Type, Name}]);
%%        _ ->
%%            riak_core_metadata:delete({split_backend, splits}, Name),
%%            case riak_core_metadata:get({split_backend, splits}, Name) of
%%                undefined ->
%%                    io:format("Succesfully removed backend: ~p~n", [{Type, Name}]),
%%                    ok;
%%                _ ->
%%                    io:format("Failed to remove Backend: ~p~n", [{Type, Name}]),
%%                    error
%%            end
%%    end.

%%put_and_confirm_metadata(Name) ->
%%    case riak_core_metadata:get({split_backend, splits}, Name) of
%%        undefined ->
%%%%            Config = riak_kv_split_backend:get_backend_config(Name, Type),
%%            riak_core_metadata:put({split_backend, splits}, Name, false),
%%            case riak_core_metadata:get({split_backend, splits}, Name) of
%%                undefined ->
%%                    io:format("Failed to create new backend type: ~p and bucket: ~p Please check logs", [false, Name]),
%%                    error;
%%                _ ->
%%                    io:format("Succesfully created backend type: ~p, Bucket: ~p~n", [false, Name]),
%%                    ok
%%            end;
%%        _ ->
%%            io:format("Backend already exists~n"),
%%            ok
%%    end.

repair_2i(["status"]) ->
    try
        Status = riak_kv_2i_aae:get_status(),
        Report = riak_kv_2i_aae:to_report(Status),
        io:format("2i repair status is running:\n~s", [Report]),
        ok
    catch
        exit:{noproc, _NoProcErr} ->
            io:format("2i repair is not running\n", []),
            ok
    end;
repair_2i(["kill"]) ->
    case whereis(riak_kv_2i_aae) of
        Pid when is_pid(Pid) ->
            try
                riak_kv_2i_aae:stop(60000)
            catch
                _:_->
                    lager:warning("Asking nicely did not work."
                                  " Will try a hammer"),
                    ok
            end,
            Mon = monitor(process, riak_kv_2i_aae),
            exit(Pid, kill),
            receive
                {'DOWN', Mon, _, _, _} ->
                    lager:info("2i repair process has been killed by user"
                               " request"),
                    io:format("The 2i repair process has ceased to be.\n"
                              "Since it was killed forcibly, you may have to "
                              "wait some time\n"
                              "for all internal locks to be released before "
                              "trying again\n", []),
                    ok
            end;
        undefined ->
            io:format("2i repair is not running\n"),
            ok
    end;
repair_2i(Args) ->
    case validate_repair_2i_args(Args) of
        {ok, IdxList, DutyCycle} ->
            case length(IdxList) < 5 of
                true ->
                    io:format("Will repair 2i on these partitions:\n", []),
                    _ = [io:format("\t~p\n", [Idx]) || Idx <- IdxList],
                    ok;
                false ->
                    io:format("Will repair 2i data on ~p partitions\n",
                              [length(IdxList)]),
                    ok
            end,
            Ret = riak_kv_2i_aae:start(IdxList, DutyCycle),
            case Ret of
                {ok, _Pid} ->
                    io:format("Watch the logs for 2i repair progress reports\n", []),
                    ok;
                {error, {lock_failed, not_built}} ->
                    io:format("Error: The AAE tree for that partition has not been built yet\n", []),
                    error;
                {error, {lock_failed, LockErr}} ->
                    io:format("Error: Could not get a lock on AAE tree for"
                              " partition ~p : ~p\n", [hd(IdxList), LockErr]),
                    error;
                {error, already_running} ->
                    io:format("Error: 2i repair is already running. Check the logs for progress\n", []),
                    error;
                {error, early_exit} ->
                    io:format("Error: 2i repair finished immediately. Check the logs for details\n", []),
                    error;
                {error, Reason} ->
                    io:format("Error running 2i repair : ~p\n", [Reason]),
                    error
            end;
        {error, aae_disabled} ->
            io:format("Error: AAE is currently not enabled\n", []),
            error;
        {error, Reason} ->
            io:format("Error: ~p\n", [Reason]),
            io:format("Usage: riak-admin repair-2i [--speed [1-100]] <Idx> ...\n", []),
            io:format("Speed defaults to 100 (full speed)\n", []),
            io:format("If no partitions are given, all partitions in the\n"
                      "node are repaired\n", []),
            error
    end.

ensemble_status([]) ->
    riak_kv_ensemble_console:ensemble_overview();
ensemble_status(["root"]) ->
    riak_kv_ensemble_console:ensemble_detail(root);
ensemble_status([Str]) ->
    N = parse_int(Str),
    case N of
        undefined ->
            io:format("No such ensemble: ~s~n", [Str]);
        _ ->
            riak_kv_ensemble_console:ensemble_detail(N)
    end.

%%%===================================================================
%%% Private
%%%===================================================================

validate_repair_2i_args(Args) ->
    case riak_kv_entropy_manager:enabled() of
        true ->
            parse_repair_2i_args(Args);
        false ->
            {error, aae_disabled}
    end.

parse_repair_2i_args(["--speed", DutyCycleStr | Partitions]) ->
    DutyCycle = parse_int(DutyCycleStr),
    case DutyCycle of
        undefined ->
            {error, io_lib:format("Invalid speed value (~s). It should be a " ++
                                  "number between 1 and 100",
                                  [DutyCycleStr])};
        _ ->
            parse_repair_2i_args(DutyCycle, Partitions)
    end;
parse_repair_2i_args(Partitions) ->
    parse_repair_2i_args(100, Partitions).

parse_repair_2i_args(DutyCycle, Partitions) ->
    case get_2i_repair_indexes(Partitions) of
        {ok, IdxList} ->
            {ok, IdxList, DutyCycle};
        {error, Reason} ->
            {error, Reason}
    end.

get_2i_repair_indexes([]) ->
    AllVNodes = riak_core_vnode_manager:all_vnodes(riak_kv_vnode),
    {ok, [Idx || {riak_kv_vnode, Idx, _} <- AllVNodes]};
get_2i_repair_indexes(IntStrs) ->
    {ok, NodeIdxList} = get_2i_repair_indexes([]),
    F =
    fun(_, {error, Reason}) ->
            {error, Reason};
       (IntStr, Acc) ->
            case parse_int(IntStr) of
                undefined ->
                    {error, io_lib:format("~s is not an integer\n", [IntStr])};
                Int ->
                    case lists:member(Int, NodeIdxList) of
                        true ->
                            Acc ++ [Int];
                        false ->
                            {error,
                             io_lib:format("Partition ~p does not belong"
                                           ++ " to this node\n",
                                           [Int])}
                    end
            end
    end,
    IdxList = lists:foldl(F, [], IntStrs),
    case IdxList of
        {error, Reason} ->
            {error, Reason};
        _ ->
            {ok, IdxList}
    end.

format_stats([], Acc) ->
    lists:reverse(Acc);
format_stats([{Stat, V}|T], Acc) ->
    format_stats(T, [io_lib:format("~p : ~p~n", [Stat, V])|Acc]).

atomify_nodestrs(Strs) ->
    lists:foldl(fun("local", Acc) -> [node()|Acc];
                   (NodeStr, Acc) -> try
                                         [list_to_existing_atom(NodeStr)|Acc]
                                     catch error:badarg ->
                                         io:format("Bad node: ~s\n", [NodeStr]),
                                         Acc
                                     end
                end, [], Strs).

print_vnode_statuses([]) ->
    ok;
print_vnode_statuses([{VNodeIndex, StatusData} | RestStatuses]) ->
    io:format("VNode: ~p~n", [VNodeIndex]),
    print_vnode_status(StatusData),
    io:format("~n"),
    print_vnode_statuses(RestStatuses).

print_vnode_status([]) ->
    ok;
print_vnode_status([{backend_status,
                     Backend,
                     StatusItem} | RestStatusItems]) ->
    if is_binary(StatusItem) ->
            StatusString = binary_to_list(StatusItem),
            io:format("Backend: ~p~nStatus: ~n~s~n",
                      [Backend, string:strip(StatusString)]);
       true ->
            io:format("Backend: ~p~nStatus: ~n~p~n",
                      [Backend, StatusItem])
    end,
    print_vnode_status(RestStatusItems);
print_vnode_status([StatusItem | RestStatusItems]) ->
    if is_binary(StatusItem) ->
            StatusString = binary_to_list(StatusItem),
            io:format("Status: ~n~s~n",
                      [string:strip(StatusString)]);
       true ->
            io:format("Status: ~n~p~n", [StatusItem])
    end,
    print_vnode_status(RestStatusItems).

bucket_error_xlate(Errors) when is_list(Errors) ->
    string:join(
      lists:map(fun bucket_error_xlate/1, Errors),
      "~n");
bucket_error_xlate({Property, not_integer}) ->
    [atom_to_list(Property), " must be an integer"];

%% `riak_kv_bucket:coerce_bool/1` allows for other values but let's
%% not encourage bad behavior
bucket_error_xlate({Property, not_boolean}) ->
    [atom_to_list(Property), " should be \"true\" or \"false\""];

bucket_error_xlate({Property, not_valid_quorum}) ->
    [atom_to_list(Property), " must be an integer or (one|quorum|all)"];
bucket_error_xlate({_Property, Error}) when is_list(Error)
                                            orelse is_binary(Error) ->
    Error;
bucket_error_xlate({Property, Error}) when is_atom(Error) ->
    [atom_to_list(Property), ": ", atom_to_list(Error)];
bucket_error_xlate({Property, Error}) ->
    [atom_to_list(Property), ": ", io_lib:format("~p", [Error])];
bucket_error_xlate(X) ->
    io_lib:format("~p", [X]).
