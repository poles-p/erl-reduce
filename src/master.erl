%% Author: Marcin Milewski (mmilewski@gmail.com),
%%         Karol Stosiek (karol.stosiek@gmail.com)
%%         Piotr Polesiuk (bassists@o2.pl)
%% Created: 21-11-2010
%% Description: Coordinates the map/reduce computation: feeds workers with data,
%%     takes care of dying workers and returns collected data to the user.
-module(master).

%%
%% Exported Functions.
%% TODO: move partition_map_data/2 to a separate module.
-export([run/5,
         partition_map_data/2,
         execute_reduce_phase/1 % exported for testing purporses.
        ]).

-record(master_state, {alive_mapper_pids, alive_reducer_pids, dead_mapper_pids = [], dead_reducer_pids = [], data_partition}).

%%
%% API Functions.
%%

%% @doc Main master function. Executes map/reduce operation on given input
%%     with given map and reduce workers. Returns overall result. We require
%%     to have at least one map and one reduce worker pid available.
%% @spec (MapWorkerPids, ReduceWorkerPids, InputData) -> FinalResult where
%%    MapWorkerPids = [pid()],
%%    ReduceWorkerPids = [pid()],
%%    InputData = [{K1,V1}],
%%    FinalResult = [{K3,V3}]
run(MainWorkerPid, MapWorkerPids, ReduceWorkerPids, InputData, Recipe)
  when length(MapWorkerPids) > 0,
       length(ReduceWorkerPids) > 0 ->
    error_logger:info_msg("Starting master (~p).", [self()]),
	State1 = #master_state{alive_mapper_pids = MapWorkerPids, 
						  alive_reducer_pids = ReduceWorkerPids,
						  data_partition = create_data_partition(InputData, MapWorkerPids)},
	create_monitors(State1),
    State2 = execute_map_phase(State1, Recipe),
    MapReduceResult = execute_reduce_phase(State2),
    MainWorkerPid ! {self(), {map_reduce_result, MapReduceResult}},
    map_reducing_complete(MapWorkerPids, ReduceWorkerPids),
    MapReduceResult.



%%
%% Local Functions.
%%

%% @doc Creates monitors for map workers and reduce workers.
%% @spec (State) -> () where
%%      State = master_state
create_monitors(State) ->
	% Create monitors for reducres
	error_logger:info_msg(
		"Creating monitors for map workers ~p", 
		[State#master_state.alive_mapper_pids]),
	spawn(monitors, monitor_map_workers, [self(), State#master_state.alive_mapper_pids]),
	
	error_logger:info_msg(
		"Creating monitors for reduce workers ~p", 
		[State#master_state.alive_reducer_pids]),
	spawn(monitors, monitor_reduce_workers, [self(), State#master_state.alive_reducer_pids]),
	ok.

%% @doc Creates partition of map data.
%% @spec (MapData, MapWorkerPids) -> dict() where
%%     MapData = [{K1,V1}],
%%     MapWorkerPids = [pid()]
%% @private
create_data_partition(MapData, MapWorkerPids) ->
	error_logger:info_msg("Partitioning"),
	MapDataParts = partition_map_data(MapData, length(MapWorkerPids)),
	
	PidDatas = lists:zip(MapWorkerPids, MapDataParts),
	
	dict:from_list(PidDatas).

%% @doc Partitions given Data into chunks of at most ChunkSize and appends
%%     resulting chunk to the Accumulator. If there is at least ChunkSize
%%     elements in the given list, then chunk size is ChunkSize; otherwise
%%     there is at most as many elements in the chunk, as in the given list.
%%     TODO: make the difference in chunk sizes be at most 1 element.
%% @spec (Data,ChunkSize,Accumulator) -> PartitionedMapData where
%%     Data = [{K1,V1}],
%%     ChunkSize = int(),
%%     Accumulator = PartitionedMapData = [Data]
partition_data_with_accumulator([], _, Accumulator) ->
    Accumulator;

partition_data_with_accumulator(Data, ChunkSize, Accumulator)
  when length(Data) < ChunkSize ->
    [Data|Accumulator];

partition_data_with_accumulator(Data, ChunkSize, Accumulator) ->
    {Chunk, OtherData} = lists:split(ChunkSize, Data),
    partition_data_with_accumulator(OtherData, ChunkSize, [Chunk|Accumulator]).


%% @doc Parititions data into almost evenly-sized data chunks. Last chunk may
%%     not be as big as other chunks.
%%     TODO: make the difference in chunk sizes be at most 1 element.
%% @spec (Data,Chunks) -> [Data] where
%%     Data = [{K1,V1}],
%%     Chunks = int()
partition_map_data(_, 0) -> [];
partition_map_data([], _) -> [];
partition_map_data(Data, 1) -> [Data];
partition_map_data(Data, Chunks) ->
    partition_data_with_accumulator(Data, round(length(Data) / Chunks), []).


%% @doc Sends given part of data to worker with given pid.
%% @spec (MapWorkerPids, MapData) -> void() where
%%      MapWorkerPids = [pid()],
%%      MapData = [{K1,V1}]
%% @private
send_data_to_map_worker(MapWorkerPid, MapData) ->
    error_logger:info_msg("Sending data to map worker ~p~n", [MapWorkerPid]),
    MapWorkerPid ! {self(), {map_data, MapData}},
    ok.


%% @doc Collects map_finished messages from map workers given in RemainingMapperPids,
%%     When any process crashed, updates state.
%% @spec (RemainingMapperPids, State) -> State where
%%     RemainingMapperPids = [pid()],
%%     State = master_state
%% @private
collect_map_finished_messages([], State) ->
	State;
collect_map_finished_messages(RemainingMapperPids, State) ->
	receive
		{MapperPid, map_finished} ->
			error_logger:info_msg("mapper ~p finished job",
								  [MapperPid]),
			collect_map_finished_messages(lists:delete(MapperPid, RemainingMapperPids), State);
		
		{MapperPid, map_worker_down} ->
			error_logger:info_msg("mapper ~p crashed",
								  [MapperPid]),
			NewState = mapper_failure_state_update(MapperPid, State),
			collect_map_finished_messages(lists:delete(MapperPid, RemainingMapperPids), NewState)
	end.

%% @doc Collects N map_finished messages, where N is length of PidDatas.
%%     When any process crashes, recalculates and spread data among alive
%%     map workers.
%% @spec (PidDatas, State) -> State where
%%      PidDatas = [{pid,MapData}],
%%      MapData = [{K1,V1}],
%%      State = master_state
%% @private
collect_map_finished_pids([], State) -> State;
collect_map_finished_pids(PidDatas, State) ->
	
	MappersWithData = lists:map(fun ({Pid, _}) -> Pid end, PidDatas),
	State2 = collect_map_finished_messages(MappersWithData, State),

	AlivePids = State2#master_state.alive_mapper_pids,
	CrashedPids = State2#master_state.dead_mapper_pids,
    
	if length(AlivePids) == 0 ->
           error_logger:error_msg("MAPPING PHASE CRASHED. Part of data "
                                      "wasn't mapped.~n"),
           State2;
       
       length(CrashedPids) > 0 ->
           error_logger:info_msg("Recomputing data from crashed mappers~n"),
           DatasListForCrashedPids = 
               [ Data || {Pid, Data} <- PidDatas,
                         lists:member(Pid, CrashedPids) ],
           DatasForCrashedPids = lists:flatten(DatasListForCrashedPids),
           RepartitionedData = partition_map_data(DatasForCrashedPids, 
                                                  length(AlivePids)),
           
           error_logger:info_msg("Data repartitioned~n"),
           PidsForRepartitionedData = lists:sublist(AlivePids,
                                                    length(RepartitionedData)),
           
           error_logger:info_msg("Mappers used for recomputation: ~p~n",
                                 [PidsForRepartitionedData]),
           NewPidDatas = lists:zip(PidsForRepartitionedData,
                                   RepartitionedData),
           lists:foreach(fun ({A,B}) -> 
                                  send_data_to_map_worker(A, B)
                         end, NewPidDatas),
           
           error_logger:info_msg("Data was sent, collecting again~n"),
		   
		   NewState = State2#master_state{dead_mapper_pids = [],
										  data_partition = dict:merge(fun (_, Data1, Data2) ->
																			   lists:append(Data1, Data2)
																	  end,
																	  State#master_state.data_partition, 
																	  dict:from_list(NewPidDatas))},
           
		   collect_map_finished_pids(NewPidDatas, NewState);
   
       true ->
           State2
    end.


%% @doc Sends partitioned data to map workers and collects results.
%% @spec (MapData, MapWorkerPids, ReduceWorkerPids, Recipe) -> State where
%%     MapData = [{K1,V1}],
%%     MapWorkerPids = [pid()],
%%     ReduceWorkerPids = [pid()],
%%     Recipe = (K2) -> ReducerPid,
%%     State = master_state
%% @private
execute_map_phase(State, Recipe) ->
    error_logger:info_msg("Starting map phase with map workers ~p",
                          [State#master_state.alive_mapper_pids]),
    
	% Spread data among the map workers.
    error_logger:info_msg("Spreading map data among map workers ~p",
                          [State#master_state.alive_mapper_pids]),
    
	dict:fold(fun (Pid, Data, _) ->
					   send_data_to_map_worker(Pid, Data)
			  end, ok, State#master_state.data_partition),
	
    % Collect map_finished messages
    error_logger:info_msg("Collecting map_finished messages..."),
    State2 = collect_map_finished_pids(dict:to_list(State#master_state.data_partition), State),
    error_logger:info_msg("Collected map_finished messages ~p out of ~p.",
                          [length(State2#master_state.alive_mapper_pids), length(State#master_state.alive_mapper_pids)]),    
	
    error_logger:info_msg("Sending mapping_phase_finished to all mappers"),
    
	lists:foreach(fun (MapperPid) ->
                           MapperPid ! {self(), mapping_phase_finished}
                  end, State2#master_state.alive_mapper_pids),
    
	error_logger:info_msg("Sending recipies to mappers"),
    lists:foreach(fun (MapperPid) ->
                           MapperPid ! {self(), {recipe, Recipe}}
                  end, State2#master_state.alive_mapper_pids),
    error_logger:info_msg("Receipes sent to all map workers"),
	
    % Collect map_send_finished messages.
    error_logger:info_msg("Collecting map_send_finished messages..."),
    ResultState = collect_map_send_finished(State2#master_state.alive_mapper_pids, State2),
    
    error_logger:info_msg("Map phase finished.", []),
	ResultState.

execute_additional_map_phase(State) ->
	AlivePids = State#master_state.alive_mapper_pids,
	
	lists:foreach(fun (Pid) ->
						   Pid ! {self(), {additional_map_phase, State#master_state.alive_reducer_pids}}
				  end, AlivePids),
	
	LostDatasList = lists:map(fun (Pid) ->
									   dict:fetch(Pid, State#master_state.data_partition)
							  end, State#master_state.dead_mapper_pids),
	LostData = lists:flatten(LostDatasList),
	
	RepartitionedData = partition_map_data(LostData, length(AlivePids)),
	PidsForRepartitionedData = lists:sublist(AlivePids, length(RepartitionedData)),

	error_logger:info_msg("Mappers used for recomputation: ~p~n",
                                 [PidsForRepartitionedData]),
	NewPidDatas = lists:zip(PidsForRepartitionedData,
							RepartitionedData),
	lists:foreach(fun ({A,B}) ->
						   send_data_to_map_worker(A, B)
				  end, NewPidDatas),
	
	error_logger:info_msg("Data was sent, collecting again~n"),
		
	State2 = State#master_state{dead_mapper_pids = [],
								data_partition = dict:merge(fun (_, Data1, Data2) ->
																	 lists:append(Data1, Data2)
															end,
															State#master_state.data_partition,
															dict:from_list(NewPidDatas))},
	
	State3 = collect_map_finished_pids(NewPidDatas, State2),
	
	error_logger:info_msg("Sending mapping_phase_finished to all mappers"),
    
	lists:foreach(fun (MapperPid) ->
                           MapperPid ! {self(), mapping_phase_finished}
                  end, State3#master_state.alive_mapper_pids),
    
	% Collect map_send_finished messages.
    error_logger:info_msg("Collecting map_send_finished messages..."),
	
    collect_map_send_finished(State3#master_state.alive_mapper_pids, State3).

%% @doc Collects data_canceled messages.
%% @spec (RemainingReducerPids, State) -> State where
%%    RemainingReducerPids = [pid()],
%%    State = master_state
%% @private
collect_data_canceled([], State) ->
	State;
collect_data_canceled(RemainingReducerPids, State) ->
	receive
		{ReducerPid, data_canceled} ->
			NewRemainingReducerPids = lists:delete(ReducerPid, RemainingReducerPids),
			
			collect_data_canceled(NewRemainingReducerPids, State);
		
		{ReducerPid, reduce_worker_down} ->
			NewRemainingReducerPids = lists:delete(ReducerPid, RemainingReducerPids),			
			NewState = reducer_failure_state_update(ReducerPid, State),
			
			collect_data_canceled(NewRemainingReducerPids, NewState)
	end.
	


%% @doc Collects map_send_finished messages. When reducer is down, sends 
%%    reduce_worker_down message to map workers.
%% @spec (MapWorkerPids, State) -> State where
%%    MapWorkerPids = [pid()],
%%    State = master_state
%% @private
collect_map_send_finished([], State) ->
	case State#master_state.dead_mapper_pids of
		[] -> State;
		_ ->
		   % cancel data from dead mappers.
		   lists:foreach(fun (ReducerPid) ->
								  ReducerPid ! {self(), {cancel_data, State#master_state.dead_mapper_pids}}
						 end, State#master_state.alive_reducer_pids),
		   
		   % collect data_canceled messages
		   State2 = collect_data_canceled(State#master_state.alive_reducer_pids, State),
		   
		   % additional map phase
		   execute_additional_map_phase(State2)
	end;
collect_map_send_finished(MapWorkerPids, State) ->
	receive
		{MapperPid, map_send_finished} ->
			NewMapWorkerPids = lists:delete(MapperPid, MapWorkerPids),
			error_logger:info_msg("Received map_send_finishd from ~p, waiting for ~p mappers.",
								  [MapperPid, length(NewMapWorkerPids)]),
			collect_map_send_finished(NewMapWorkerPids, State);
		
		{ReducerPid, reduce_worker_down} ->
			% inform mappers about dead reducer.
			lists:foreach(fun(MapWorkerPid) ->
								  MapWorkerPid ! {ReducerPid, reduce_worker_down}
						  end, MapWorkerPids),
			
			NewState = reducer_failure_state_update(ReducerPid, State),
			collect_map_send_finished(MapWorkerPids, NewState);
		
		{MapperPid, map_worker_down} ->
			NewMapWorkerPids = lists:delete(MapperPid, MapWorkerPids),
			
			NewState = mapper_failure_state_update(MapperPid, State),
			
			collect_map_send_finished(NewMapWorkerPids, NewState)
	end.


%% @doc Executes reduction phase of the map/reduce operation.
%% @spec (State) -> FinalResult where
%%     State = master_state,
%%     FinalResult = [{K3,V3}]
%% @private
execute_reduce_phase(State) ->
    error_logger:info_msg("Starting reduce phase with reduce workers ~p",
                          [State#master_state.alive_reducer_pids]),
	
    % Initiate reduction.
    error_logger:info_msg("Sending start signal to reduce workers ~p", 
                          [State#master_state.alive_reducer_pids]),
    lists:foreach(fun (ReducerPid) ->
                           error_logger:info_msg(
                             "Sending start signal to reduce worker ~p",
                             [ReducerPid]),
                           
                           ReducerPid ! {self(), start_reducing}
                  end, 
				  State#master_state.alive_reducer_pids),
    
    % Collect and return final results.
    error_logger:info_msg("Collecting final results from reduce workers ~p",
                          [State#master_state.alive_reducer_pids]),
	
    collect_reduce_phase_results(State#master_state.alive_reducer_pids, [], State).

execute_additional_reduce_phase(CollectedResults, DeadReducerPids, State) ->
	error_logger:info_msg("Starting additional reduce phase; dead reduce workers : ~p",
						  [DeadReducerPids]),
	
	% Create a new recipe.
	Recipe = conf:recipe(State#master_state.alive_reducer_pids),
	
	% Send recipe to mappers.
	error_logger:info_msg("Sending the recipe to mappers ~p", 
						  [State#master_state.alive_mapper_pids]),
	lists:foreach(fun (MapperPid) ->
						   MapperPid ! {self(), {recipe, Recipe, DeadReducerPids, State#master_state.alive_reducer_pids}}
				  end, State#master_state.alive_mapper_pids),
	
	% Collect 'map_send_finished' messages
	State2 = collect_map_send_finished(State#master_state.alive_mapper_pids, 
										 State),
	
	% Send start signal to reduce workers
	error_logger:info_msg("Sending start signal to reduce workers ~p", 
                          [State2#master_state.alive_reducer_pids]),
    lists:foreach(fun (ReducerPid) ->
                           error_logger:info_msg(
                             "Sending start signal to reduce worker ~p",
                             [ReducerPid]),
                           
                           ReducerPid ! {self(), start_reducing}
                  end, 
				  State2#master_state.alive_reducer_pids),
    
    % Collect and return final results.
    error_logger:info_msg("Collecting final results from reduce workers ~p",
                          [State2#master_state.alive_reducer_pids]),
	
    collect_reduce_phase_results(State2#master_state.alive_reducer_pids, CollectedResults, State2).

%% @doc Collects all results of reduce phase. When one of reduce wokres fails,
%%    function starts additional reduce phase.
%% @spec (RemainingReducerPids, Accumulator, State) -> FinalResult where
%%     RemainingReducerPids = [pid()],
%%     Accumulator = [{K3,V3}],
%%     State = master_state,
%%     FinalResult = [{K3,V3}]
%% @private
collect_reduce_phase_results([], Accumulator, State) ->
	case State#master_state.dead_reducer_pids of
		[] ->
			Accumulator;
		DeadReducerPids ->
			NewState = State#master_state{dead_reducer_pids = []},
			execute_additional_reduce_phase(Accumulator, DeadReducerPids, NewState)
	end;
collect_reduce_phase_results(RemainingReducerPids, Accumulator, State) ->
	receive
		{ReducerPid, {reduce_finished, ReduceResult}} ->
			error_logger:info_msg(
			  "Received final data from reducer ~p.", 
			  [ReducerPid]),
			
			NewRemainingReducerPids = lists:delete(ReducerPid, RemainingReducerPids),
			collect_reduce_phase_results(NewRemainingReducerPids, ReduceResult ++ Accumulator, State);
		
		{ReducerPid, reduce_worker_down} ->
			error_logger:info_msg(
			  "Reduce worker ~p is down",
			  [ReducerPid]),
			
			NewRemainingReducerPids = lists:delete(ReducerPid, RemainingReducerPids),
			NewState = reducer_failure_state_update(ReducerPid, State),
			
			collect_reduce_phase_results(NewRemainingReducerPids, Accumulator, NewState)
	end.
%% @doc Updates state after reduce wokrer failure. 
%% @spec (ReducerPid, State) -> State where
%%     ReducerPid = pid(),
%%     State = master_state
%% @private
reducer_failure_state_update(ReducerPid, State) ->
	AliveReducerPids = State#master_state.alive_reducer_pids,
	DeadReducerPids  = State#master_state.dead_reducer_pids,
	State#master_state{alive_reducer_pids = lists:delete(ReducerPid, AliveReducerPids),
					   dead_reducer_pids = [ReducerPid | DeadReducerPids]}.

%% @doc Updates state after map wokrer failure. 
%% @spec (MapperPid, State) -> State where
%%     ReducerPid = pid(),
%%     State = master_state
%% @private
mapper_failure_state_update(MapperPid, State) ->
	AliveMapperPids = State#master_state.alive_mapper_pids,
	DeadMapperPids  = State#master_state.dead_mapper_pids,
	State#master_state{alive_mapper_pids = lists:delete(MapperPid, AliveMapperPids),
					   dead_mapper_pids = [MapperPid | DeadMapperPids]}.

%% @doc Sends 'map_reducing_complete' message to all workers.
%% @spec (MapWorkerPids, ReduceWorkerPids) -> () where
%%     MapWorkerPids = [pid()],
%%     ReduceWorkerPids = [pid()]
map_reducing_complete(MapWorkerPids, ReduceWorkerPids) ->
	error_logger:info_msg("Map-Reduce complete. Notifying mappers ~p and reducers ~p.",
						 [MapWorkerPids, ReduceWorkerPids]),
	
	lists:foreach(fun (WorkerPid) -> 
						   WorkerPid ! {self(), map_reducing_complete}
				  end, MapWorkerPids ++ ReduceWorkerPids).
