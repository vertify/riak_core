%% -------------------------------------------------------------------
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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
%%
%% @doc Throttling for activities that can potentially overload resources.
%% Provides support for "throttling" (i.e. slowing down) calling processes
%% that, when under heavy load, could potentially overload other resources.
%% Activities are identified by a unique key so that different throttle values
%% can be used for different activities.
%%
%% The throttle for an activity can be set to a specific value with the
%% `set_throttle/2' function, or it can be set to a value based on the current
%% "load" for the activity by using the `set_load_map/2' function to define a
%% mapping from load factors to throttle values, and then periodically calling
%% the `set_throttle_by_load/2' function with the current load factor.
-module(riak_core_throttle).

%% API
-export([set_throttle/2,
         clear_throttle/1,
         set_load_map/2,
         clear_load_map/1,
         set_throttle_by_load/2,
         throttle/1]).

-ifdef(TEST).
-export([get_throttle/1, get_throttle_for_load/2]).
-define(SLEEP(Time), Time).
-else.
-define(SLEEP(Time), timer:sleep(Time)).
-endif.

-type activity_key() :: atom().
-type throttle_time() :: non_neg_integer().
-type load_factor() :: integer().
-type load_map() :: [{load_factor(), throttle_time()}, ...].

-export_type([activity_key/0, throttle_time/0, load_factor/0, load_map/0]).

-define(THROTTLE_KEY(Key), list_to_atom("throttle_" ++ atom_to_list(Key))).
-define(THROTTLE_LOAD_MAP_KEY(Key),
        list_to_atom("throttle_load_map" ++ atom_to_list(Key))).

%% @doc Sets the throttle for the activity identified by `Key' to the specified
%% `Time'.
-spec set_throttle(activity_key(), throttle_time()) -> ok.
set_throttle(Key, Time) when Time >= 0 ->
    application:set_env(riak_core, ?THROTTLE_KEY(Key), Time).

%% @doc Clears the throttle for the activity identified by `Key'.
-spec clear_throttle(activity_key()) -> ok.
clear_throttle(Key) ->
    application:unset_env(riak_core, ?THROTTLE_KEY(Key)).

%% @doc Sets the load map for the activity identified by `Key'. The load map is
%% a mapping from load factors to throttle values. Once this mapping has been
%% established, the `set_throttle_by_load/2' function can be used to set the
%% throttle for the activity based on the current load factor.
%% @see set_throttle_by_load/2
-spec set_load_map(activity_key(), load_map()) -> ok.
set_load_map(Key, LoadMap) ->
    application:set_env(riak_core,
                        ?THROTTLE_LOAD_MAP_KEY(Key),
                        lists:sort(LoadMap)).

%% @doc Clears the load map for the activity identified by `Key'.
-spec clear_load_map(activity_key()) -> ok.
clear_load_map(Key) ->
    application:unset_env(riak_core, ?THROTTLE_LOAD_MAP_KEY(Key)).

%% @doc Sets the throttle for the activity identified by `Key' to a value
%% determined by consulting the load map for the activity. The throttle value
%% is the value associated with the largest load factor <= `LoadFactor'.
%% If there is no load map defined for the activity, exits with
%% error({no_load_map, Key}).
%% @see set_load_map/2
-spec set_throttle_by_load(activity_key(), load_factor()) ->
    throttle_time().
set_throttle_by_load(Key, LoadFactor) when is_integer(LoadFactor) ->
    case get_throttle_for_load(Key, LoadFactor) of
        undefined ->
            error({no_load_map, Key});
        ThrottleTime ->
            set_throttle(Key, ThrottleTime),
            ThrottleTime
    end.

%% @doc Sleep for the number of milliseconds specified as the throttle time for
%% the activity identified by `Key'. If there is no throttle value configured
%% for `Key', then exits with error({badkey, Key}).
-spec throttle(activity_key()) -> throttle_time().
throttle(Key) ->
    case get_throttle(Key) of
        undefined ->
            error({badkey, Key});
        Time ->
            ?SLEEP(Time),
            Time
    end.

-spec get_throttle(activity_key()) -> throttle_time() | undefined.
get_throttle(Key) ->
    application:get_env(riak_core, ?THROTTLE_KEY(Key), undefined).

-spec get_throttle_for_load(activity_key(), load_factor()) ->
    throttle_time() | undefined.
get_throttle_for_load(Key, LoadFactor) ->
    case application:get_env(riak_core, ?THROTTLE_LOAD_MAP_KEY(Key)) of
        {ok, LoadMap} ->
            find_throttle_for_load_factor(LoadMap, LoadFactor);
        _ ->
            undefined
    end.

find_throttle_for_load_factor([], _LoadFactor) ->
    undefined;
find_throttle_for_load_factor(LoadMap, LoadFactor) ->
    Candidates = lists:takewhile(
        fun({Load, _Time}) ->
            LoadFactor >= Load
        end,
        LoadMap),
    {_Load, ThrottleTime} = case Candidates of
        [] ->
            lists:nth(1, LoadMap);
        _ ->
            lists:last(Candidates)
    end,
    ThrottleTime.
