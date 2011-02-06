%% Author: Piotr Polesiuk (bassists@o2.pl),
%%         Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 25-12-2010
%% Description: Implementation of reduce worker, performing the reduce operation
%%    on given input.
-module(reduce_worker).

%%
%% Exported Functions.
%%
-export([run/1,
		 collect_reduce_data/0]). % exported for testing purposes


%%
%% API Functions.
%%

%% @doc Represents a single reduce worker node. See the documentation for
%%     protocol definition.
%% @spec ((IntermediateData) -> FinalData) -> () where
%%     IntermediateData = {K2,[V2]},
%%     FinalData = [{K3,V3}]
run(ReduceFunction) ->
    error_logger:info_msg("Reduce worker ~p started. Waiting for reduce "
                              "data...", [self()]),
    
	reducer_main_loop(ReduceFunction).

%%
%% Local Functions.
%%

%% @doc Main loop of reduce worker.
%% @spec ((IntermediateData) -> FinalData) -> () where
%%     IntermediateData = {K2,[V2]},
%%     FinalData = [{K3,V3}]
reducer_main_loop(ReduceFunction) ->
	CollectionResult = collect_reduce_data(),
	case CollectionResult of
		map_reducing_complete ->
			error_logger:info_msg("Map-reducing finished. Quitting.", []);
		
		{data_collected, MasterPid, ReduceData} ->
			ReduceResult = ReduceFunction(ReduceData),
    		error_logger:info_msg("Reducing finished; notifying master (~p)."
								 " and waiting for new instructions",
								 [MasterPid]),
    		MasterPid ! {self(), {reduce_finished, ReduceResult}},
			
			reducer_main_loop(ReduceFunction)
	end.

%% @doc Receives map results from mappers and collects them into accumulator 
%%     (CollectedResultsDict) until receives start_reducing message from master.
%%     Function returns master PID and collected data or 'map_reducing_complete' 
%%     term, when computation is complete.
%% @spec Accumulator -> Result where
%%     Accumulator = dictionary(),
%%     Result = {'data_collected', MasterPid, CollectedData} | 'map_reducing_complete',
%%     MasterPid = pid(),
%%     CollectedData = [{K2,[V2]}]
%% @private
collect_reduce_data_loop(CollectedResultsDict) ->
    receive
        {MapperPid, {reduce_data, ReduceData}} ->
            error_logger:info_msg("Received data from map worker ~p.",
                                  [MapperPid]),
            
            NewCollectedResults = 
                lists:foldl(fun({Key, Value}, Dict) ->
                                    dict:append_list(Key, [{MapperPid, Value}], Dict)
                            end, 
                            CollectedResultsDict, ReduceData),
            
            error_logger:info_msg("Sending acknowledgement to map worker ~p",
                                  [MapperPid]),
            
            MapperPid ! {self(), reduce_data_acknowledged},
            collect_reduce_data_loop(NewCollectedResults);
        
        {MasterPid, start_reducing} ->
            error_logger:info_msg("Collected reduce data; received start "
                                      "signal from master (~p).",
                                  [MasterPid]),
			
			DataWithoutPids = dict:fold(
								fun (Key, PidValueList, Acc) ->
										 [{Key,lists:map(fun ({_, Value}) -> Value end, PidValueList)} | Acc]
								end, [], CollectedResultsDict),
            
            {data_collected, MasterPid, DataWithoutPids};

		{MasterPid, {cancel_data, MapperPids}} ->
			error_logger:info_msg("Map worker failure. Cancel data from ~p.",
                                  [MapperPids]),
			
			MasterPid ! {self(), data_canceled},
			
			NewCollectedResults =
				dict:map(fun (_, PidValueList) ->
								  lists:filter(fun ({Pid, _}) ->
														lists:all(fun (DeadMapperPid) ->
																		   DeadMapperPid /= Pid
																  end, MapperPids)
											   end,	PidValueList)
						 end, CollectedResultsDict),
			
			collect_reduce_data_loop(NewCollectedResults);

		{_, map_reducing_complete} ->
			map_reducing_complete
    end.


%% @doc Collects map results from mappers until receives 
%%     start_reducing message from master. Function returns master PID
%%     and collected data or 'map_reducing_complete' term, when computation is complete.
%% @spec () -> Result where
%%     Result = {'data_collected', MasterPid, CollectedData} | 'map_reducing_complete',
%%     MasterPid = pid(),
%%     CollectedData = [{K2,[V2]}]
%% @private
collect_reduce_data() ->
    error_logger:info_msg("Collecting reduce data..."),
    
    collect_reduce_data_loop(dict:new()).
