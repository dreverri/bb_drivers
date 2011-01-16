-module(riak_ruby_node).
-export([test/0]).

test() ->
  Cmd = "ruby riak-ruby-client-node.rb",
  Port = open_port({spawn, Cmd}, [{packet, 4}, nouse_stdio, exit_status, binary]),
  Payload = term_to_binary({link_walk, <<"test">>, <<"1">>, <<"link">>, <<"_">>}),
  port_command(Port, Payload),
  receive
    {Port, {data, Data}} ->
      Reply = binary_to_term(Data),
      io:format("~p~n", [Reply])
  end.
