package com.atguigu.function;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

public class OpListStateMapFunction implements MapFunction<WaterSensor, Integer>, CheckpointedFunction {

    private ListState<Integer> listState;
    private Integer count = 0;

    @Override
    public Integer map(WaterSensor value) throws Exception {
        count++;
        return count;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("snapshotState>>>>>>>>");
        listState.clear();
        listState.add(count);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("initializeState>>>>>");

        ListState<Integer> state = context.getOperatorStateStore().getListState(new ListStateDescriptor<Integer>("list-state", Integer.class));

        Iterable<Integer> iterable = state.get();
        for (Integer integer : iterable) {
            count += integer;
        }
    }
}
