%%-------------------------------------------------------------------
%%
%% riak_kv_qry_snapshot: Riak SQL query result disk-based temp storage
%%                       (aka 'query buffers' or 'snapshots')
%%
%% Copyright (C) 2016 Basho Technologies, Inc. All rights reserved
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
%%-------------------------------------------------------------------

%% @doc SELECT queries with a LIMIT clause are persisted on disk, in
%%      order to (a) effectively enable sorting of big amounts of data
%%      and (b) support paging, whereby subsequent, separate queries
%%      can extract a subrange ("SELECT * FROM T LIMIT 10" followed by
%%      "SELECT * FROM T LIMIT 10 OFFSET 10").
%%
%%      Disk-backed temporary storage is implemented as per-query
%%      instance of leveldb (a "query buffer").  Once qry_worker has
%%      finished collecting data from vnodes, the resulting set of
%%      chunks is stored in a single leveldb table.  Exported
%%      functions are provided to extract certain subranges from it
%%      and thus to execute any subsequent queries.
%%
%%      Queries are matched by identical SELECT, FROM, WHERE, GROUP BY
%%      and ORDER BY expressions.  A hashing function on these query
%%      parts is provided for query identification and quick
%%      comparisons.

-module(riak_kv_qry_snapshot).

-behaviour(gen_server).

-export([start_link/1,
         init/1,
         handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3
        ]).

-export([
         get_or_create_qbuf/2,  %% new Query arrives, with some Options
         delete_qbuf/1,         %% drop a table by Ref
         get_qbufs/0,           %% list all query buffer refs and is_ flags in a proplist
         add_chunk/2,           %% new Chunk collected by worker for a query Ref
         qbuf_exists/1,         %% is there a temp table for this Query?
         is_qbuf_ready/1,       %% are there results ready for this Query?
         get_qbuf_orig_query/1, %% original query used at creation of this qbuf
         get_qbuf_range/1,      %% get the WHERE range for this query Ref
         get_qbuf_size/1,       %% table size (as reported by leveldb:get_size, or really the file size)
         get_total_size/0,      %% get total size of all temp tables

         query/2,               %% effectively, only INSERT (short-circuits to add_chunk) and a limited SELECT are allowed

         %% utility functions
         make_qref/1,
         wait_qbuf_ready/1      %% blocking wait until results are ready
        ]).

-type qbuf_ref() :: binary().
-type watermark_status() :: underfull | limited_capacity.  %% overengineering much?

-export_type([qbuf_ref/0, watermark_status/0]).

-type timestamp() :: integer().
-type row_data() :: [[riak_pb_ts_codec:ldbvalue()]].

-include("riak_kv_ts.hrl").

-define(SERVER, ?MODULE).

%% defaults for #state{} fields, settable in the Options proplist for
%% gen_server:init/1
-define(SOFT_WATERMARK, 1024*1024*1024).  %% 1G
-define(HARD_WATERMARK, 4096*1024*1024).  %% 4G
-define(INCOMPLETE_QBUF_RELEASE_MSEC, 1*60*1000).  %% clean up incomplete buffers since last add_chunk
-define(QBUF_EXPIRE_MSEC, 5*60*1000).              %% drop buffers clients are not interested in


%%%===================================================================
%%% API
%%%===================================================================

-spec get_or_create_qbuf(?SQL_SELECT{}, proplists:proplist()) ->
                                {ok, qbuf_ref()} |
                                {error, query_non_pageable|no_space}.
%% @doc (Maybe create and) return a new query buffer and set it up for
%%      receiving chunks of data from qry_worker.
get_or_create_qbuf(SQL, Options) ->
    gen_server:call(?SERVER, {get_or_create_qbuf, SQL, Options}).

-spec delete_qbuf(qbuf_ref()) -> ok.
%% @doc Dispose of this query buffer (do nothing if it does not exist).
delete_qbuf(QBufRef) ->
    gen_server:call(?SERVER, {delete_qbuf, QBufRef}).

-spec get_qbufs() -> [{qbuf_ref(), proplists:proplist()}].
%% @doc List active qbufs, with properties
get_qbufs() ->
    gen_server:call(?SERVER, get_qbufs).

-spec add_chunk(qbuf_ref(), row_data()) ->
                       {ok, watermark_status()} | {error, bad_qbuf_ref|overfull}.
%% @doc Add a chunk to this query buffer. Report watermark status with ok or error.
add_chunk(QBufRef, Data) ->
    gen_server:call(?SERVER, {add_chunk, QBufRef, Data}).

-spec qbuf_exists(qbuf_ref()) -> boolean().
%% @doc Check if there is a query buffer for this Query.
qbuf_exists(QBufRef) ->
    gen_server:call(?SERVER, {qbuf_exists, QBufRef}).

-spec is_qbuf_ready(qbuf_ref()) -> boolean().
%% @doc Check if results are ready for this Query.
is_qbuf_ready(QBufRef) ->
    gen_server:call(?SERVER, {is_qbuf_ready, QBufRef}).

-spec get_qbuf_orig_query(qbuf_ref()) -> {ok, ?SQL_SELECT{}} | {error, bad_qbuf_ref}.
%% @doc Get the original SQL used when this qbuf was created.
get_qbuf_orig_query(QBufRef) ->
    gen_server:call(?SERVER, {get_qbuf_orig_query, QBufRef}).

-spec get_qbuf_range(qbuf_ref()) ->
                            {ok, {timestamp(), timestamp()}} | {error, bad_qbuf_ref}.
%% @doc Get the original WHERE range for this query (from the SQL stored at creation).
get_qbuf_range(QBufRef) ->
    gen_server:call(?SERVER, {get_qbuf_range, QBufRef}).

-spec get_qbuf_size(qbuf_ref()) ->
                           {ok, non_neg_integer()} | {error, bad_qbuf_ref}.
%% @doc Report the size on disk of the backing files for this query.
get_qbuf_size(QBufRef) ->
    gen_server:call(?SERVER, {get_qbuf_size, QBufRef}).

-spec get_total_size() -> non_neg_integer().
%% @doc Compute the total size of all temp tables.
get_total_size() ->
    gen_server:call(?SERVER, get_total_size).

-spec fetch_limit(qbuf_ref(), ?SQL_SELECT{}) ->
                         {ok, row_data(), Remaining::non_neg_integer()} |
                         {error, bad_qbuf_ref|bad_sql}.
%% @doc Fetch our precious records, LIMIT of them starting from OFFSET
%%      as given in the SQL, additionally returning the number of records remaining.
fetch_limit(QBufRef, SQL) ->
    gen_server:call(?SERVER, {fetch_limit, QBufRef, SQL}).

-spec get_cursor(qbuf_ref()) -> non_neg_integer().
%% @doc Get a position for the next fetch.
get_cursor(QBufRef) ->
    gen_server:call(?SERVER, {get_cursor, QBufRef}).


-spec make_qref(?SQL_SELECT{}) -> {ok, qbuf_ref()} | {error, query_non_pageable}.
%% @doc Make a query ref, to identify this (and similar) queries as
%%      having certain SELECT, FROM and WHERE clauses.
make_qref(SQL) ->
    {ok, stub}.

-spec wait_qbuf_ready(qbuf_ref(), TimeoutMsec::integer()) ->
                             ok | {error, bad_qbuf_ref|timeout}.
%% @doc Blocking wait until results are ready on this query buffer.
wait_qbuf_ready(QBufRef, TimeoutMsec) ->
    {error, stub}.


-record(ldb_ref, {
          ref :: eleveldb:db_ref(),
          data_root :: string()
         }).

-record(qbuf, {
          %% original SELECT query this table holds the data for
          orig_qry :: ?SQL_SELECT{},
          %% a DDL for it
          ddl :: ?DDL{},

          %% leveldb handle for the temp storage
          ldb_ref :: #ldb_ref{},

          %% ranges accumulated so far
          ranges = [] :: [{timestamp(), timestamp()}],
          %% entire range as given in WHERE clause
          where_range :: {timestamp(), timestamp(),

          %% true if the ranges set has a single pair, and it covers
          %% the entire WHERE range
          is_complete = false :: boolean(),

          %% total records stored (when complete)
          total_records = 0 :: non_neg_integer(),
          %% cursor position
          cursor_position = 0 :: non_neg_integer(),

          %% total size on disk, for reporting
          size = 0 :: non_neg_integer(),

          %% last added chunk (when not complete) or queried
          last_updated = 0 :: timestamp()
         }).

-record(state, {
          qbufs = [] :: [{qbuf_ref(), #qbuf{}}],
          total_size = 0 :: non_neg_integer(),
          %% no new queries; accumulation allowed
          soft_watermark :: non_neg_integer(),
          %% drop some tables now
          hard_watermark :: non_neg_integer(),
          %% drop incomplete query buffer after this long since last add_chunk
          incomplete_qbuf_release_msec :: non_neg_integer(),
          %% drop complete query buffers after this long since serving last query
          qbuf_expire_msec :: non_neg_integer()
         }).


-spec start_link(RegisteredName::atom()) -> {ok, pid()} | ignore | {error, term()}.
start_link(RegisteredName) ->
    gen_server:start_link({local, RegisteredName}, ?MODULE, [], []).


-spec init(Options) -> {ok, #state{}}.
%% @private
init(Options) ->
    State = #state{soft_watermark =
                       proplists:get_value(soft_watermark, Options, ?SOFT_WATERMARK),
                   hard_watermark =
                       proplists:get_value(soft_watermark, Options, ?HARD_WATERMARK),
                   incomplete_qbuf_release_msec =
                       proplists:get_value(incomplete_qbuf_release_msec, Options, ?INCOMPLETE_QBUF_RELEASE_MSEC),
                   qbuf_expire_msec =
                       proplists:get_value(qbuf_expire_msec, Options, ?QBUF_EXPIRE_MSEC),
    schedule_tick(),
    {ok, State}.


-spec handle_call(term(), pid(), #state{}) -> {reply, term(), #state{}}.
%% @private
handle_call(get_qbufs, _From, State) ->
    do_get_qbufs(State);

handle_call(get_total_size, _From, State) ->
    do_get_total_size(State);

handle_call({get_or_create_qbuf, SQL, Options}, _From, State) ->
    do_get_or_create_qbuf(SQL, Options, State);

handle_call({delete_qbuf, QBufRef}, _From, State) ->
    do_delete_qbuf(QBufRef, State);

handle_call({qbuf_exists, QBufRef}, _From, State) ->
    do_qbuf_exists(QBufRef, State);

handle_call({add_chunk, QBufRef, Data}, _From, State) ->
    do_add_chunk(QBufRef, Data, State);

handle_call({get_qbuf_size, QBufRef}, _From, State) ->
    do_get_qbuf_size(QBufRef, State);

handle_call({get_qbuf_range, QBufRef}, _From, State) ->
    do_get_qbuf_range(QBufRef, State);

handle_call({get_qbuf_orig_query, QBufRef}, _From, State) ->
    do_get_qbuf_orig_query(QBufRef, State);

handle_call({is_qbuf_ready, QBufRef}, _From, State) ->
    do_is_qbuf_ready(QBufRef, State);

handle_call({fetch_limit, QBufRef, SQL}, _From, State) ->
    do_fetch_limit(QBufRef, SQL, State);

handle_call({get_cursor, QBufRef}, _From, State) ->
    do_get_cursor(QBufRef, State).


-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
%% @private
handle_cast(Msg, State) ->
    lager:info("Not handling cast message ~p", [Msg]),
    {noreply, State}.

-spec terminate(term(), #state{}) -> term().
%% @private
terminate(_Reason, _State) ->
    ok.

-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% do_thing functions
%%%===================================================================

do_get_qbufs(State#state{qbufs = QBufs}) ->
    Res = [{Ref, [{orig_qry, OrigQry},
                  {is_complete, IsComplete},
                  {last_updated, LastUpdated},
                  {total_records, TotalRecords}]}
           || {Ref, #qbuf{orig_qry = OrigQry,
                          is_complete = IsComplete,
                          last_updated = LastUpdated,
                          total_records = TotalRecords}} <- QBufs],
    {reply, Res, State}.

do_get_total_size(State#state{total_size = TotalSize}) ->
    {reply, TotalSize, State}.

do_get_or_create_qbuf(SQL, Options, State0#state{qbufs = QBufs0,
                                                 soft_watermark = SoftWMark}) ->
    case ensure_qref(SQL, QBufs0) of
        {ok, {existing, QBufRef}} ->
            {reply, {ok, QBufRef}, State0};
        {ok, {new, QBufRef}} ->
            TotalSize = get_total_size(QBufs0),
            if TotalSize > SoftWMark ->
                    {reply, {error, no_space}, State0};
               el/=se ->
                    case ldb_new_table(QBufRef) of
                        {ok, LdbRef} ->
                            QBuf = #qbuf{orig_qry = SQL,
                                         where_range = get_query_where_range(SQL),
                                         ldb_ref = LdbRef,
                                         last_updated = os:timestamp()},
                            QBufs = QBufs0 ++ [QBuf],
                            State = State0#state{qbufs = QBufs,
                                                 total_size = compute_total_qbuf_size(QBufs)},
                            {reply, {ok, QBufRef}, State};
                        {error, Reason} ->
                            {reply, {error, Reason}, State0}
                    end
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State0}
    end.

ensure_qref(SQL, QBufs) ->
    case make_qref(SQL) of
        {ok, QBufRef} ->
            case get_qbuf_record(QBufRef, QBufs) of
                {_Ref, QBuf} ->
                    {ok, {existing, QBufRef}};
                false ->
                    {ok, {new, QBufRef}}
            end;
        {error, query_non_pageable} = ErrorReason ->
            ErrorReason
    end.

do_delete_qbuf(QBufRef, State0#state{qbufs = QBufs0}) ->
    case release_qbuf(
           get_qbuf_record(QBufRef, QBufs0)) of
        ok ->
            {reply, ok, State0#state{qbufs = lists:keydelete(QBufRef, 1, QBufs0)}};
        {error, Reason} ->
            {reply, {error, Reason}, State0}
    end.

do_qbuf_exists(QBufRef, State#state{qbufs = QBufs}) ->
    case get_qbuf_record(QBufRef, QBufs) of
        false ->
            {reply, false, State};
        _QBuf ->
            {reply, true, State}
    end.

do_add_chunk(QBufRef, Data, State0#state{qbufs = QBufs0,
                                         total_size = TotalSize,
                                         hard_watermark = HardWatermark}) ->
    case get_qbuf_record(QBufRef, QBufs) of
        false ->
            {reply, {error, bad_qbuf_ref}, State0};
        QBuf0 ->
            case maybe_add_chunk(QBuf0, Data, TotalSize, HardWatermark) of
                {ok, QBuf} ->
                    State = State0#state{qbufs = lists:keyreplace(
                                                   QBufRef, 1, QBufs0, {QBufRef, QBuf})},
                    {reply, ok, State};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end
    end.

maybe_add_chunk(QBuf0#qbuf{ldb_ref       = LdbRef,
                           where_range   = WhereRange,
                           total_records = TotalRecords0},
                Data,
                TotalRecords0, HardWatermark) ->
    DataSize = compute_chunk_size(Data),
    if TotalSize + DataSize > HardWatermark ->
            {error, no_space};
       el/=se ->
            case ldb_add_records(LdbRef, Data) of
                ok ->
                    OnDiskSize = ldb_get_size(LdbRef),  %% that will probably be different from DataSize, due to slack
                    Ranges = ldb_get_ranges(LdbRef),
                    IsComplete =
                        case Ranges of
                            [SingleContiguous] when SingleContiguous == WhereRange ->
                                true;
                            _DisjointOrIncomplete ->
                                false
                        end,
                    QBuf = QBuf0#qbuf{total_records = TotalRecords0 + length(Data),
                                      size = OnDiskSize,
                                      ranges = Ranges,
                                      is_complete = IsComplete,
                                      last_updated = os:timestamp()},
                    {ok, QBuf}
            end
    end.

do_get_qbuf_size(QBufRef, State#state{qbufs = QBufs}) ->
    case get_qbuf_record(QBufRef, QBufs) of
        false ->
            {reply, {error, bad_qbuf_ref}, State};
        #qbuf{size = Size} ->
            {reply, {ok, Size}, State}
    end.

do_get_qbuf_range(QBufRef, State#state{qbufs = QBufs}) ->
    case get_qbuf_record(QBufRef, QBufs) of
        false ->
            {reply, {error, bad_qbuf_ref}, State};
        #qbuf{where_range = WhereRange} ->
            {reply, {ok, WhereRange}, State}
    end.

do_get_qbuf_orig_query(QBufRef, State#state{qbufs = QBufs}) ->
    case get_qbuf_record(QBufRef, QBufs) of
        false ->
            {reply, {error, bad_qbuf_ref}, State};
        #qbuf{orig_qry = OrigQry} ->
            {reply, {ok, OrigQry}, State}
    end.

do_is_qbuf_ready(QBufRef, State0#state{}) ->
    {State, }.

do_fetch_limit(QBufRef, SQL, State0#state{}) ->
    {State, }.

do_get_cursor(QBufRef, State0#state{}) ->
    {State, }.


%%%===================================================================
%%% other internal functions
%%%===================================================================

%% query properties

get_query_where_range(SQL) ->
    
    .

%% data ops
compute_chunk_size(Data) ->
    %% do something more clever than size(term_to_binary(Data))
    
    42.


%% buffer list maintenance

compute_total_qbuf_size(QBufs) ->
    lists:foldl(
      fun({_Ref, #qbuf{size = Size}}, Acc) -> Acc + Size end,
      0, QBufs).

get_qbuf_record(Ref, QBufs) ->
    lists:keyfind(Ref, 1, QBufs).

release_qbuf(false) ->
    ok;
release_qbuf(#qbuf{ldb_ref = LdbRef}) ->
    
    ok.

%% ldb interface
ldb_new_table(QBufRef) ->
    
    LdbRef.

ldb_get_size(LdbRef) ->
    
    0.

ldb_get_ranges(LdbRef) ->
    
    [].
