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
                mapred_query}).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    error_logger:info_msg("starting riak-java-client node~n", []),
    CmdDir = filename:join([priv_dir(), "riak-java-client-jinterface-node"]),
    Cmd = filename:join([CmdDir, "riak-java-client-jinterface-node.sh"]),
    Name = "riak-java-client-" ++ integer_to_list(Id) ++ "@127.0.0.1",
    Url = basho_bench_config:get(riak_url, "http://127.0.0.1:8098/riak"),
    Args = [Name, Url],
    case catch erlang:open_port({spawn_executable, Cmd}, [stderr_to_stdout,
                                                          {args, Args},
                                                          {cd, CmdDir}]) of
        {'EXIT', Error} ->
            error_logger:error_msg("Could not start riak-java-client node: ~p~n", [Error]),
            {stop, Error};
        Port when is_port(Port) ->
            Bucket  = basho_bench_config:get(riak_bucket, <<"test">>),
            MapRedQuery = basho_bench_config:get(mapred_query, undefined),
            {ok, #state{node = {mbox, list_to_atom(Name)},
                        bucket = Bucket,
                        mapred_query = MapRedQuery}}
    end.

run(mapred, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case State#state.mapred_query of
        undefined ->
            {error, "mapred_query undefined", State};
        MapRedQuery ->
            MapRed = {struct,
                      [{<<"inputs">>, [[State#state.bucket, key_to_binary(Key)]]},
                       {<<"query">>, MapRedQuery}]},
            State#state.node ! {self(), mapred, mochijson2:encode(MapRed)},
            receive
                ok ->
                    {ok, State};
                error ->
                    {error, "MapReduce failed", State}
            end
    end.


%% Convert different key types to binaries
%% "1" -> <<"1">>
%% 1 -> <<"1">>
%% Useful for accessing the object via HTTP
key_to_binary(Key) when is_list(Key) ->
    list_to_binary(Key);
key_to_binary(Key) when is_integer(Key) ->
    list_to_binary(integer_to_list(Key));
key_to_binary(Key) when is_binary(Key) ->
    Key;
key_to_binary(Key) ->
    term_to_binary(Key).

priv_dir() ->
    case code:priv_dir(qilr) of
        {error, bad_name} ->
            Path0 = filename:dirname(code:which(?MODULE)),
            Path1 = filename:absname_join(Path0, ".."),
            filename:join([Path1, "priv"]);
        Path ->
            filename:absname(Path)
    end.
