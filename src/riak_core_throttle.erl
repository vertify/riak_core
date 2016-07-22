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
%% "load" for the activity by using the `set_limits/2' function to define a
%% mapping from load factors to throttle values, and then periodically calling
%% the `set_throttle_by_load/2' function with the current load factor. Note
%% that "load factor" is an abstract concept, the purpose of which is simply
%% to represent numerically the amount of load that an activity is under and
%% map that level of load to a particular throttle level. Examples of metrics
%% that could be used for load factors include the size of the mailbox for a
%% particular process, or the number of messages per second being sent to an
%% external service such as Solr.
-module(riak_core_throttle).

%% API
-export([get_throttle/1,
         set_throttle/2,
         clear_throttle/1,
         disable_throttle/1,
         enable_throttle/1,
         is_throttle_enabled/1,
         init/4,
         set_limits/2,
         get_limits/1,
         clear_limits/1,
         create_limits_translator_fun/2,
         set_throttle_by_load/2,
         throttle/1]).

-ifdef(TEST).
-export([get_throttle_for_load/2]).
-define(SLEEP(Time), Time).
-else.
-define(SLEEP(Time), timer:sleep(Time)).
-endif.

-type activity_key() :: atom().
-type throttle_time() :: non_neg_integer().
-type load_factor() :: number() | atom() | tuple().
-type limits() :: [{load_factor(), throttle_time()}].

-export_type([activity_key/0, throttle_time/0, load_factor/0, limits/0]).

-define(THROTTLE_KEY(Key), list_to_atom("throttle_" ++ atom_to_list(Key))).
-define(THROTTLE_LIMITS_KEY(Key),
        list_to_atom("throttle_limits_" ++ atom_to_list(Key))).
-define(THROTTLE_KILL_KEY(Key),
        list_to_atom("throttle_kill_" ++ atom_to_list(Key))).

%% @doc Sets the throttle for the activity identified by `Key' to the specified
%% `Time'.
-spec set_throttle(activity_key(), throttle_time()) -> ok.
set_throttle(Key, Time) when Time >= 0 ->
    application:set_env(riak_core, ?THROTTLE_KEY(Key), Time).

%% @doc Clears the throttle for the activity identified by `Key'.
-spec clear_throttle(activity_key()) -> ok.
clear_throttle(Key) ->
    application:unset_env(riak_core, ?THROTTLE_KEY(Key)).

%% @doc Disables the throttle for the activity identified by `Key'.
-spec disable_throttle(activity_key()) -> ok.
disable_throttle(Key) ->
    application:set_env(riak_core, ?THROTTLE_KILL_KEY(Key), true).

%% @doc Enables the throttle for the activity identified by `Key'.
-spec enable_throttle(activity_key()) -> ok.
enable_throttle(Key) ->
    application:unset_env(riak_core, ?THROTTLE_KILL_KEY(Key)).

%% @doc Returns `true' if the throttle for the activity identified by `Key' is
%% enabled, `false' otherwise.
-spec is_throttle_enabled(activity_key()) -> boolean().
is_throttle_enabled(Key) ->
    Killed = application:get_env(riak_core, ?THROTTLE_KILL_KEY(Key), false),
    not Killed.

%% @doc Initializes the throttle for the activity identified by `Key' using
%% configuration values found in the application environment for `AppName'.
-spec init(Key::activity_key(),
           AppName::atom(),
           {LimitsKey::atom(), LimitsDefault::limits()},
           {KillSwitchKey::atom(), KillSwitchDefault::boolean()}) ->
    ok | {error, Reason :: term()}.
init(Key,
     AppName,
     {LimitsKey, LimitsDefault},
     {KillSwitchKey, KillSwitchDefault}) ->
    Limits = app_helper:get_env(AppName, LimitsKey, LimitsDefault),
    set_limits(Key, Limits),

    KillSwitch = app_helper:get_env(AppName, KillSwitchKey, KillSwitchDefault),
    case KillSwitch of
        true ->
            disable_throttle(Key);
        false ->
            enable_throttle(Key)
    end.

%% @doc <p>Sets the limits for the activity identified by `Key'. `Limits' is
%% a mapping from load factors to throttle values. Once this mapping has been
%% established, the `set_throttle_by_load/2' function can be used to set the
%% throttle for the activity based on the current load factor.</p>
%%
%% <p>All of the throttle values in the `Limits' list must be non-negative
%% integers. Additionally, the `Limits' list must contain a tuple with the
%% key `-1'. If either of these conditions does not hold, this function exits
%% with an error.</p>
%% @end
%%
%% @see set_throttle_by_load/2
-spec set_limits(activity_key(), limits()) -> ok | no_return().
set_limits(Key, Limits) ->
    ok = validate_limits(Key, Limits),
    application:set_env(riak_core,
                        ?THROTTLE_LIMITS_KEY(Key),
                        lists:sort(Limits)).

%% @doc Returns the limits for the activity identified by `Key' if defined,
%% otherwise returns `undefined'.
-spec get_limits(activity_key()) -> limits() | undefined.
get_limits(Key) ->
    case application:get_env(riak_core, ?THROTTLE_LIMITS_KEY(Key)) of
        {ok, Limits} ->
            Limits;
        _ ->
            undefined
    end.

%% @doc Clears the limits for the activity identified by `Key'.
-spec clear_limits(activity_key()) -> ok.
clear_limits(Key) ->
    application:unset_env(riak_core, ?THROTTLE_LIMITS_KEY(Key)).

%% @doc Returns a fun that used in Cuttlefish translations to translate from
%% configuration items of the form:
%%  `ConfigPrefix'.throttle.tier1.`LoadFactorMeasure'
%% to the list of tuples form expected by the `set_limits/2' function in this
%% module. See riak_kv.schema and yokozuna.schema for example usages.
create_limits_translator_fun(ConfigPrefix, LoadFactorMeasure) ->
    fun(Conf) ->
            %% Grab all of the possible names of tiers so we can ensure that
            %% both mailbox_size and delay are included for each tier.
            TierNamesM = cuttlefish_variable:fuzzy_matches([ConfigPrefix, "throttle", "$tier", LoadFactorMeasure], Conf),
            TierNamesD = cuttlefish_variable:fuzzy_matches([ConfigPrefix, "throttle", "$tier", "delay"], Conf),
            TierNames = lists:usort(TierNamesM ++ TierNamesD),
            Throttles = lists:sort(lists:foldl(
                                     fun({"$tier", Tier}, Settings) ->
                                             Mbox = cuttlefish:conf_get([ConfigPrefix, "throttle", Tier, LoadFactorMeasure], Conf),
                                             Delay = cuttlefish:conf_get([ConfigPrefix, "throttle", Tier, "delay"], Conf),
                                             [{Mbox - 1, Delay}|Settings]
                                     end, [], TierNames)),
            case Throttles of
                %% -1 is a magic "minimum" bound and must be included, so if it
                %% isn't present we call it invalid
                [{-1,_}|_] ->
                    Throttles;
                _ ->
                    Msg = ConfigPrefix
                        ++ ".throttle tiers must include a tier with "
                        ++ LoadFactorMeasure
                        ++ " 0",
                    cuttlefish:invalid(Msg)
            end
    end.

%% @doc <p>Sets the throttle for the activity identified by `Key' to a value
%% determined by consulting the limits for the activity. The throttle value
%% is the value associated with the largest load factor <= `LoadFactor'.
%% Normally the `LoadFactor' will be a number representing the current level
%% of load for the activity, but it is also allowed to pass an atom or a tuple
%% as the %% `LoadFactor'. This can be used when a numeric value cannot be
%% established, %% and results in using the throttle value for the largest load
%% factor defined %% in the limits for the activity.</p>
%%
%% <p>If there are no limits defined for the activity, exits with
%% error({no_limits, Key}).</p>
%% @see set_limits/2
-spec set_throttle_by_load(activity_key(), load_factor()) ->
    throttle_time().
set_throttle_by_load(Key, LoadFactor) ->
    case get_throttle_for_load(Key, LoadFactor) of
        undefined ->
            error({no_limits, Key});
        ThrottleVal ->
            set_throttle(Key, ThrottleVal),
            ThrottleVal
    end.

%% @doc Sleep for the number of milliseconds specified as the throttle time for
%% the activity identified by `Key' (unless the throttle for the activity has
%% been disabled with `disable_throttle/1'). If there is no throttle value
%% configured for `Key', then exits with error({badkey, Key}).
-spec throttle(activity_key()) -> throttle_time().
throttle(Key) ->
    case get_throttle(Key) of
        undefined ->
            error({badkey, Key});
        Time ->
            case is_throttle_enabled(Key) of
                true ->
                    ?SLEEP(Time),
                    Time;
                false ->
                    0
            end
    end.

-spec get_throttle(activity_key()) -> throttle_time() | undefined.
get_throttle(Key) ->
    application:get_env(riak_core, ?THROTTLE_KEY(Key), undefined).

-spec get_throttle_for_load(activity_key(), load_factor()) ->
    throttle_time() | undefined.
get_throttle_for_load(Key, LoadFactor) ->
    case get_limits(Key) of
        undefined ->
            undefined;
        Limits ->
            find_throttle_for_load_factor(Limits, LoadFactor)
    end.

find_throttle_for_load_factor([], _LoadFactor) ->
    undefined;
find_throttle_for_load_factor(Limits, LoadFactor) when is_number(LoadFactor) ->
    Candidates = lists:takewhile(
        fun({Load, _Time}) ->
            LoadFactor >= Load
        end,
        Limits),
    {_Load, ThrottleVal} = case Candidates of
        [] ->
            lists:nth(1, Limits);
        _ ->
            lists:last(Candidates)
    end,
    ThrottleVal;
find_throttle_for_load_factor(Limits, LoadFactor)
  when is_atom(LoadFactor); is_tuple(LoadFactor) ->
    {_Load, ThrottleVal} = lists:last(Limits),
    ThrottleVal.

validate_limits(Key, Limits) ->
    Validators = [validate_all_non_negative(Limits),
                  validate_has_negative_one_key(Limits)],
    Errors = [Message || {error, Message} <- Validators],
    case Errors of
        [] ->
            ok;
        Messages ->
            lager:error("***** Invalid throttle limits for ~p: ~p.",
                        [Key, Messages]),
            error(invalid_throttle_limits)
    end.

validate_all_non_negative(Limits) ->
    Good = lists:all(
      fun({LoadFactor, ThrottleVal}) ->
              is_integer(LoadFactor) andalso
              is_integer(ThrottleVal) andalso
              ThrottleVal >= 0
      end,
      Limits),
    case Good of
        true ->
            ok;
        false ->
            {error, "All throttle values must be non-negative integers"}
    end.

validate_has_negative_one_key(Limits) ->
    case lists:keyfind(-1, 1, Limits) of
        {-1, _} ->
            ok;
        false ->
            {error, "Must include -1 entry"}
    end.
