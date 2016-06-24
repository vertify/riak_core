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
%% -------------------------------------------------------------------
-module(riak_core_throttle_tests).

-include_lib("eunit/include/eunit.hrl").

activity_keys() ->
    [some_activity, another_activity].

clear_throttles(ActivityKeys) ->
    lists:foreach(fun(Key) ->
                          riak_core_throttle:clear_throttle(Key),
                          riak_core_throttle:clear_load_map(Key)
                  end,
                  ActivityKeys).

throttle_test_() ->
    {foreach,
     fun activity_keys/0,
     fun clear_throttles/1,
     [fun test_throttle_badkey/1,
      fun test_set_throttle/1,
      fun test_set_throttle_by_load_with_no_load_map/1,
      fun test_set_throttle_by_load_with_empty_load_map/1,
      fun test_set_throttle_by_load_with_good_load_map/1,
      fun test_set_throttle_by_load_actually_sets_throttle/1,
      fun test_set_load_map_does_not_overwrite_current_throttle/1
     ]}.

test_throttle_badkey([Key|_]) ->
    [?_assertError({badkey, Key}, riak_core_throttle:throttle(Key))].

test_set_throttle([Key1, Key2|_]) ->
    ok = riak_core_throttle:set_throttle(Key1, 42),
    [?_assertEqual(42, riak_core_throttle:throttle(Key1)),
     ?_assertError({badkey, Key2}, riak_core_throttle:throttle(Key2))].

test_set_throttle_by_load_with_no_load_map([Key|_]) ->
    [?_assertError({no_load_map, Key},
                   riak_core_throttle:set_throttle_by_load(Key, 50))].

test_set_throttle_by_load_with_empty_load_map([Key|_]) ->
    ok = riak_core_throttle:set_load_map(Key, []),
    [?_assertError({no_load_map, Key},
                   riak_core_throttle:set_throttle_by_load(Key, 50))].

test_set_throttle_by_load_with_good_load_map([Key, Key2|_]) ->
    LoadMap = [{-1, 0}, {10, 5}, {100, 10}],
    ok = riak_core_throttle:set_load_map(Key, LoadMap),
    [
     ?_assertEqual(0, riak_core_throttle:set_throttle_by_load(Key, -10000000)),
     ?_assertEqual(0, riak_core_throttle:set_throttle_by_load(Key, -10)),
     ?_assertEqual(0, riak_core_throttle:set_throttle_by_load(Key, -1)),
     ?_assertEqual(0, riak_core_throttle:set_throttle_by_load(Key, 0)),
     ?_assertEqual(0, riak_core_throttle:set_throttle_by_load(Key, 1)),
     ?_assertEqual(0, riak_core_throttle:set_throttle_by_load(Key, 2)),
     ?_assertEqual(0, riak_core_throttle:set_throttle_by_load(Key, 9)),
     ?_assertEqual(5, riak_core_throttle:set_throttle_by_load(Key, 10)),
     ?_assertEqual(5, riak_core_throttle:set_throttle_by_load(Key, 11)),
     ?_assertEqual(5, riak_core_throttle:set_throttle_by_load(Key, 99)),
     ?_assertEqual(10, riak_core_throttle:set_throttle_by_load(Key, 100)),
     ?_assertEqual(10, riak_core_throttle:set_throttle_by_load(Key, 101)),
     ?_assertEqual(10, riak_core_throttle:set_throttle_by_load(Key, 10000001)),
     ?_assertError({no_load_map, Key2},
                   riak_core_throttle:set_throttle_by_load(Key2, 100))
    ].

test_set_throttle_by_load_actually_sets_throttle([Key|_]) ->
    ok = riak_core_throttle:set_load_map(Key, [{0, 0}, {5, 42}]),
    42 = riak_core_throttle:set_throttle_by_load(Key, 5),
    [?_assertEqual(42, riak_core_throttle:throttle(Key))].

test_set_load_map_does_not_overwrite_current_throttle([Key|_]) ->
    ok = riak_core_throttle:set_load_map(Key, [{0, 0}, {100, 42}]),
    42 = riak_core_throttle:set_throttle_by_load(Key, 500),
    ok = riak_core_throttle:set_load_map(Key, [{0, 0}, {100, 500}]),
    [?_assertEqual(42, riak_core_throttle:throttle(Key))].
