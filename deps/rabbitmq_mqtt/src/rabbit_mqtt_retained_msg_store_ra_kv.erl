%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Cluster-wide retained message store backed by ra_kv.
%%
%% The dets and ets backends are rooted at rabbit:data_dir(), so a message
%% retained through one node is invisible to a SUBSCRIBE that lands on another,
%% and is lost when that node is gone. This backend keeps one Raft cluster per
%% virtual host, addressing both points 1 and 2 of the TODO at the top of
%% rabbit_mqtt_retained_msg_store.
%%
%% Requires ra >= 3.1.10 for ra_kv.

-module(rabbit_mqtt_retained_msg_store_ra_kv).

-behaviour(rabbit_mqtt_retained_msg_store).

-include("rabbit_mqtt_packet.hrl").
-include_lib("kernel/include/logger.hrl").

-export([new/2, recover/2, insert/3, lookup/2, delete/2, info/1, terminate/1]).

%% coordination is the ra system RabbitMQ uses for metadata-like clusters such
%% as rabbit_stream_coordinator, as opposed to quorum_queues.
-define(RA_SYSTEM, coordination).
-define(TIMEOUT, 30_000).

-record(store_state, {cluster_name :: atom(),
                      server_id :: ra:server_id()}).

-opaque state() :: #store_state{}.
-export_type([state/0]).

-spec new(file:name_all(), rabbit_types:vhost()) -> state().
new(_Dir, VHost) ->
    Name = cluster_name(VHost),
    Members = [{Name, N} || N <- rabbit_nodes:list_members()],
    case ra_kv:start_cluster(?RA_SYSTEM, Name, #{members => Members}) of
        {ok, Started, NotStarted} ->
            ?LOG_INFO("MQTT retained message store ~s started members ~p, "
                      "pending members ~p", [Name, Started, NotStarted]);
        {error, cluster_not_formed} ->
            %% A peer may have formed the cluster already, in which case the
            %% local member is running and usable.
            ?LOG_INFO("MQTT retained message store ~s not formed locally, "
                      "assuming an existing cluster", [Name])
    end,
    #store_state{cluster_name = Name,
                 server_id = {Name, node()}}.

%% Always reporting uninitialized routes startup through new/2, which is safe
%% because ra_kv:start_cluster/3 restarts members that already exist and leaves
%% their Raft log intact.
%%
%% The caller therefore receives an empty expire map. Rebuilding it needs the
%% set of retained topics, and ra_kv exposes no enumeration (overview/1 reports
%% num_keys only), so Message-Expiry-Interval timers are not re-armed after a
%% restart. The dets and ets backends re-arm them via
%% rabbit_mqtt_retained_msg_store:expire/2, which folds over the whole table.
-spec recover(file:name_all(), rabbit_types:vhost()) ->
    {error, uninitialized}.
recover(_Dir, _VHost) ->
    {error, uninitialized}.

-spec insert(topic(), mqtt_msg(), state()) -> ok.
insert(Topic, Msg, #store_state{server_id = ServerId}) ->
    case ra_kv:put(ServerId, Topic, Msg, ?TIMEOUT) of
        {ok, _Meta} ->
            ok;
        Err ->
            ?LOG_ERROR("MQTT retained message store failed to insert ~tp: ~tp",
                       [Topic, Err]),
            ok
    end.

-spec lookup(topic(), state()) -> mqtt_msg() | undefined.
lookup(Topic, #store_state{server_id = ServerId}) ->
    case ra_kv:get(ServerId, Topic, ?TIMEOUT) of
        {ok, _Meta, Msg} ->
            Msg;
        {error, not_found} ->
            undefined;
        Err ->
            ?LOG_ERROR("MQTT retained message store failed to look up ~tp: ~tp",
                       [Topic, Err]),
            undefined
    end.

-spec delete(topic(), state()) -> ok.
delete(Topic, #store_state{server_id = ServerId}) ->
    case ra_kv:delete(ServerId, Topic, ?TIMEOUT) of
        {ok, _Meta} ->
            ok;
        {error, not_found} ->
            ok;
        %% delete/3 attaches the leader to a miss, unlike get/3.
        {error, {not_found, _Leader}} ->
            ok;
        Err ->
            ?LOG_ERROR("MQTT retained message store failed to delete ~tp: ~tp",
                       [Topic, Err]),
            ok
    end.

%% size_bytes is always 0 because ra_kv:apply/3 keeps only {Index, Hash} per key
%% and drops the size carried in the put meta, so the total is unrecoverable.
%% retained_message_store_max_size_bytes cannot be enforced on this backend.
-spec info(state()) -> rabbit_mqtt_retained_msg_store:store_info().
info(#store_state{server_id = ServerId}) ->
    Count = case ra_kv:member_overview(ServerId) of
                #{machine := #{num_keys := N}} ->
                    N;
                _ ->
                    0
            end,
    #{count => Count,
      size_bytes => 0}.

%% The Raft cluster outlives the retainer process. Stopping members here would
%% discard the replicated state this backend exists to provide.
-spec terminate(state()) -> ok.
terminate(_State) ->
    ok.

-spec cluster_name(rabbit_types:vhost()) -> atom().
cluster_name(VHost) ->
    <<Num:128>> = erlang:md5(VHost),
    list_to_atom("mqtt_retained_kv_" ++ integer_to_list(Num)).
