-module(riak_java_client).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([Name|_]=Args) ->
    CmdDir = filename:join([priv_dir(), "riak-java-client-jinterface-node"]),
    Cmd = filename:join([CmdDir, "riak-java-client-jinterface-node.sh"]),
    Opts =  [stderr_to_stdout,
             {args, Args},
             {cd, CmdDir}],
    case start_riak_java_client_node(Cmd, Opts) of
        ok ->
            Node = {mbox, list_to_atom(Name)},
            Node ! {self(), link_process},
            {ok, Args};
        {error, Error} ->
            {stop, Error}
    end.

handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

priv_dir() ->
    case code:priv_dir(?MODULE) of
        {error, bad_name} ->
            Path0 = filename:dirname(code:which(?MODULE)),
            Path1 = filename:absname_join(Path0, ".."),
            filename:join([Path1, "priv"]);
        Path ->
            filename:absname(Path)
    end.

start_riak_java_client_node(Cmd, Options) ->
    error_logger:info_msg("starting riak-java-client node~n", []),
    case catch erlang:open_port({spawn_executable, Cmd}, Options) of
        {'EXIT', Error} ->
            {error, Error};
        Port ->
            receive
                {Port, {data, _}} ->
                    ok
            after 5000 ->
                    {error, timeout}
            end
    end.
