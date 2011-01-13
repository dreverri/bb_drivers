%% -------------------------------------------------------------------
%%
%% basho_bench_driver_riakc_pb: Driver for riak protocol buffers client
%%
%% Copyright (c) 2009 Basho Techonologies
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

%% Driver to preload Riak cluster with objects
%% optional: create objects with links
-module(bb_driver_preload).

-export([new/1,
         run/4]).

-record(state, { pid,
                 bucket,
                 link_bucket,
                 link_count,
                 r,
                 w,
                 dw,
                 rw}).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(riakc_pb_socket) of
        non_existing ->
            error_logger:error_msg("Could not load riakc_pb_socket~n"),
            halt(1);
        _ ->
            ok
    end,

    Ips  = basho_bench_config:get(riakc_pb_ips, [{127,0,0,1}]),
    Port  = basho_bench_config:get(riakc_pb_port, 8087),
    %% riakc_pb_replies sets defaults for R, W, DW and RW.
    %% Each can be overridden separately
    Replies = basho_bench_config:get(riakc_pb_replies, 2),
    R = basho_bench_config:get(riakc_pb_r, Replies),
    W = basho_bench_config:get(riakc_pb_w, Replies),
    DW = basho_bench_config:get(riakc_pb_dw, Replies),
    RW = basho_bench_config:get(riakc_pb_rw, Replies),
    Bucket  = basho_bench_config:get(riakc_pb_bucket, <<"test">>),
    LinkBucket  = basho_bench_config:get(link_bucket, <<"link">>),
    LinkCount = basho_bench_config:get(link_count, {random, uniform, [100]}),

    %% Choose the node using our ID as a modulus
    TargetIp = lists:nth((Id rem length(Ips)+1), Ips),

    case riakc_pb_socket:start_link(TargetIp, Port) of
        {ok, Pid} ->
            {ok, #state { pid = Pid,
                          bucket = Bucket,
                          link_bucket = LinkBucket,
                          link_count = LinkCount,
                          r = R,
                          w = W,
                          dw = DW,
                          rw = RW
                         }};
        {error, _Reason2} ->
            halt(1)
    end.

run(create, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    Value = ValueGen(),
    case create(to_binary(Key), to_binary(Value), State) of
        ok ->
            {ok, State};
        Reason ->
            {error, Reason, State}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

create(Key, Value, State) ->
    Pid = State#state.pid,
    Bucket = State#state.bucket,
    N = number_of_links(State#state.link_count),
    Links = lists:map(fun(I) -> create_link(<<Key/binary, "-", (to_binary(I))/binary>>, Value, State) end, lists:seq(1, N)),
    O = riakc_obj:new(Bucket, Key, Value),
    MD = dict:from_list([{<<"Links">>, to_links(Links)},
                         {<<"content-type">>, "application/x-erlang-binary"}]),
    Obj = riakc_obj:update_metadata(O, MD),
    riakc_pb_socket:put(Pid, Obj).

create_link(Key, Value, State) ->
    Pid = State#state.pid,
    LinkBucket = State#state.link_bucket,
    Link = riakc_obj:new(LinkBucket, Key, Value),
    {ok, Obj} = riakc_pb_socket:put(Pid, Link, [return_body]),
    Obj.

to_links(Objs) ->
    lists:map(fun to_link/1, Objs).

to_link(Obj) ->
    {{riakc_obj:bucket(Obj), riakc_obj:key(Obj)}, <<"tag">>}.

number_of_links(Count) when is_integer(Count) ->
    Count;

number_of_links({M, F, A}) ->
    apply(M, F, A).

%% Convert integer and list to binaries
%% "1" -> <<"1">>
%% 1 -> <<"1">>
%% Useful for accessing the object via HTTP
to_binary(Key) when is_list(Key) ->
    list_to_binary(Key);
to_binary(Key) when is_integer(Key) ->
    list_to_binary(integer_to_list(Key)).
