%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_queue_type_enabled_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TYPES, [rabbit_classic_queue, rabbit_quorum_queue, rabbit_stream_queue]).

all() ->
    [{group, enablement}].

groups() ->
    [{enablement, [],
      [
       all_types_enabled_by_default,
       disabling_one_type_leaves_the_others_enabled,
       declare_refuses_a_disabled_type
      ]}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Testcase, Config) ->
    unset_all(),
    Config.

end_per_testcase(_Testcase, _Config) ->
    unset_all(),
    ok.

all_types_enabled_by_default(_Config) ->
    lists:foreach(fun(T) -> ?assert(rabbit_queue_type:is_enabled(T)) end, ?TYPES),
    ok.

disabling_one_type_leaves_the_others_enabled(_Config) ->
    application:set_env(rabbit, stream_queues_enabled, false),
    ?assertNot(rabbit_queue_type:is_enabled(rabbit_stream_queue)),
    ?assert(rabbit_queue_type:is_enabled(rabbit_classic_queue)),
    ?assert(rabbit_queue_type:is_enabled(rabbit_quorum_queue)),
    ok.

declare_refuses_a_disabled_type(_Config) ->
    application:set_env(rabbit, stream_queues_enabled, false),
    ?assertMatch({protocol_error, internal_error, _, _},
                 rabbit_queue_type:declare(
                   queue(<<"qt-disabled">>, rabbit_stream_queue), node())),
    ok.

%%----------------------------------------------------------------------------

unset_all() ->
    application:unset_env(rabbit, classic_queues_enabled),
    application:unset_env(rabbit, quorum_queues_enabled),
    application:unset_env(rabbit, stream_queues_enabled),
    ok.

queue(NameBin, Type) ->
    amqqueue:new(rabbit_misc:r(<<"/">>, queue, NameBin),
                 _Pid = none,
                 _Durable = true,
                 _AutoDelete = false,
                 _Owner = none,
                 _Args = [],
                 <<"/">>,
                 #{},
                 Type).
