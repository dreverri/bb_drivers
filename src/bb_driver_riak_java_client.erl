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
-module(bb_driver_riak_java_client).

-export([new/1,
         run/4]).

-record(state, {node,
                bucket,
                link_walk_bucket,
                link_walk_tag
               }).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    case net_kernel:start(['basho_bench@127.0.0.1']) of
        {ok, _} ->
            error_logger:info_msg("Net kernel started as ~p\n", [node()]);
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            error_logger:error_msg("Failed to start net_kernel for ~p: ~p\n",
                                   [?MODULE, Reason]),
            halt(1)
    end,

    Name = "riak-java-client-" ++ integer_to_list(Id) ++ "@127.0.0.1",
    Url = basho_bench_config:get(riak_url, "http://127.0.0.1:8098/riak"),
    Args = [Name, Url],
    case riak_java_client:start_link(Args) of
        {ok, _} ->
            Node = {mbox, list_to_atom(Name)},
            Bucket  = basho_bench_config:get(riak_bucket, <<"test">>),
            LinkWalkBucket  = basho_bench_config:get(link_walk_bucket, <<"link">>),
            LinkWalkTag  = basho_bench_config:get(link_walk_tag, <<"_">>),

            {ok, #state{node = Node,
                        bucket = Bucket,
                        link_walk_bucket = LinkWalkBucket,
                        link_walk_tag = LinkWalkTag
                       }};
        {error, Error} ->
            error_logger:error_msg("Could not start riak-java-client node: ~p~n",
                                   [Error]),
            {stop, Error}
    end.

run(link_walk, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    Bucket = State#state.link_walk_bucket,
    Tag = State#state.link_walk_tag,
    Msg = {self(), link_walk, [State#state.bucket, key_to_binary(Key), Bucket, Tag]},
    State#state.node ! Msg,
    wait_for_reply(State).

wait_for_reply(State) ->
    receive
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State};
        error ->
            {error, "No reason given", State}
    end.

%% Convert list and integers to binaries
%% "1" -> <<"1">>
%% 1 -> <<"1">>
%% Useful for accessing the object via HTTP
key_to_binary(Key) when is_list(Key) ->
    list_to_binary(Key);
key_to_binary(Key) when is_integer(Key) ->
    list_to_binary(integer_to_list(Key)).
