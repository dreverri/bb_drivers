-module(lorem_ipsum_valgen).

-export([value/1, value/2]).

-ifdef(TEST).
-compile(export_all).
-endif.

value(Id) ->
    value(Id, 1024).

value(_Id, Size) ->
    Lorem = filename:join([priv_dir(), "lorem_ipsum.txt"]),
    {ok, Bin} = file:read_file(Lorem),
    fun() -> binary_to_list(data_block({size(Bin), Bin}, Size)) end.

priv_dir() ->
    case code:priv_dir(?MODULE) of
        {error, bad_name} ->
            Path0 = filename:dirname(code:which(?MODULE)),
            Path1 = filename:absname_join(Path0, ".."),
            filename:join([Path1, "priv"]);
        Path ->
            filename:absname(Path)
    end.

data_block({SourceSz, Source}, BlockSize) ->
    case SourceSz - BlockSize > 0 of
        true ->
            Offset = random:uniform(SourceSz - BlockSize),
            <<_:Offset/bytes, Slice:BlockSize/bytes, _Rest/binary>> = Source,
            Slice;
        false ->
            error_logger:warning_msg("source text size is too small; it needs more than ~p bytes.\n",
                  [BlockSize]),
            Source
    end.
