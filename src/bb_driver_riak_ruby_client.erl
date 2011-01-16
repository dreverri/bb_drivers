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
-module(bb_driver_riak_ruby_client).

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

new(_) ->
    case riak_ruby_client:start_link([]) of
        {ok, Pid} ->
            Bucket  = basho_bench_config:get(riak_bucket, <<"test">>),
            LinkWalkBucket  = basho_bench_config:get(link_walk_bucket, <<"link">>),
            LinkWalkTag  = basho_bench_config:get(link_walk_tag, <<"_">>),
            {ok, #state{node=Pid,
                        bucket = Bucket,
                        link_walk_bucket = LinkWalkBucket,
                        link_walk_tag = LinkWalkTag
                        }};
        _ ->
            error_logger:error_msg("failed to start ruby client node")
    end.

run(link_walk, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    Bucket = State#state.link_walk_bucket,
    Tag = State#state.link_walk_tag,
    case riak_ruby_client:link_walk(State#state.node,
                               State#state.bucket,
                               key_to_binary(Key),
                               Bucket,
                               Tag) of
        ok ->
            {ok, State};
        error ->
            {error, "unknown", State}
    end.

%% Convert list and integers to binaries
%% "1" -> <<"1">>
%% 1 -> <<"1">>
%% Useful for accessing the object via HTTP
key_to_binary(Key) when is_list(Key) ->
    list_to_binary(Key);
key_to_binary(Key) when is_integer(Key) ->
    list_to_binary(integer_to_list(Key)).
