-module(riak_ruby_client).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1,
        link_walk/5
        ]).

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

-record(state, {port}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%% returns ok|error
link_walk(Pid, Bucket, Key, LinkBucket, Tag) ->
    gen_server:call(Pid, {link_walk, Bucket, Key, LinkBucket, Tag}, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_) ->
    CmdDir = filename:join([priv_dir(), "riak-ruby-client-node"]),
    Cmd = filename:join([CmdDir, "riak-ruby-client-node.rb"]),
    Opts = [{packet, 4}, nouse_stdio, exit_status, binary],
    case catch erlang:open_port({spawn_executable, Cmd}, Opts) of
        {'EXIT', Error} ->
            {stop, Error};
        Port ->
            {ok, #state{port=Port}}
    end.

handle_call({link_walk, Bucket, Key, LinkBucket, Tag}, _From, State) ->
    Port = State#state.port,
    Msg = term_to_binary({link_walk, Bucket, Key, LinkBucket, Tag}),
    erlang:port_command(Port, Msg),
    Reply = receive
                {Port, {data, Data}} ->
                    binary_to_term(Data)
            end,
    {reply, Reply, State};

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
