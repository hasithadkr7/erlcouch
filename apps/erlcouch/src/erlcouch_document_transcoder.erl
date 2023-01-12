%%%-------------------------------------------------------------------
%%% @author hasitha
%%% @copyright (C) 2023, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Jan 2023 11:27 PM
%%%-------------------------------------------------------------------
-module(erlcouch_document_transcoder).
-author("hasitha").

-export([encode_value/2, decode_value/2, flag/1]).

-define(STANDARD_FLAG, raw_binary).

%% Flags values are taken from
%% https://docs.couchbase.com/java-sdk/2.7/nonjson.html

-define(JSON_FLAG, 16#02 bsl 24).
-define(RAW_FLAG, 16#03 bsl 24).
-define(STR_FLAG, 16#04 bsl 24).
-define(NO_FLAG, 0).

-type encoder() :: json | raw_binary | str | none | standard.
-type value() :: binary().

-spec encode_value(encoder(), value()) -> value().
encode_value(Encoders, Value) ->
  encode_value1(flag(Encoders), Value).

-spec encode_value1(integer(), value()) -> value().
encode_value1(?STR_FLAG, Value) when is_binary(Value) ->
  Value;
encode_value1(?JSON_FLAG, Value) ->
  eutils_json:encode(Value);
encode_value1(?RAW_FLAG, Value) when is_binary(Value) ->
  Value;
encode_value1(Flag, _) ->
  error({unsupported_flag, Flag}).

-spec decode_value(integer(), value()) -> value().
decode_value(?RAW_FLAG, Value) ->
  Value;
decode_value(?JSON_FLAG, Value) ->
  eutils_json:decode(Value);
decode_value(?STR_FLAG, Value) ->
  Value;
decode_value(Flag, _) ->
  error({unsupported_flag, Flag}).

-spec flag(encoder()) -> integer().
flag(none)       -> 0;
flag(standard)   -> flag(?STANDARD_FLAG);
flag(json)       -> ?JSON_FLAG;
flag(raw_binary) -> ?RAW_FLAG;
flag(str)        -> ?STR_FLAG.
