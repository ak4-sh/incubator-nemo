package org.apache.nemo.runtime.master;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Task;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class MasterUtils {

  public static Pair<Map<String, Integer>, List<Task>> getMaxMigrationCntPerStage(final ExecutorRepresenter executor,
                                                                                  final List<Double> ratios,
                                                                                  final List<String> stages) {
    final Map<String, Integer> stageIdCounterMap = new HashMap<>();
    final List<Task> tasksToBeMoved = new LinkedList<>();

    executor.getScheduledTasks().stream()
      .filter(task -> stages.contains(task.getStageId()))
      .map(task -> {
        tasksToBeMoved.add(task);
        return task.getStageId();
      })
      .forEach(stageId -> {
        stageIdCounterMap.putIfAbsent(stageId, 0);
        stageIdCounterMap.put(stageId, stageIdCounterMap.get(stageId) + 1);
      });

    for (final String key : stageIdCounterMap.keySet()) {
      final int index = stages.indexOf(key);
      final double ratio = ratios.get(index);
      final int count = stageIdCounterMap.get(key);
      final int scaledCount;
      if (ratio >= 1.0) {
        scaledCount = count;
      } else if (ratio <= 0.0) {
        scaledCount = 0;
      } else {
        // Ensure at least 1 task is moved for any positive ratio
        scaledCount = Math.max(1, (int) Math.ceil(count * ratio));
      }
      stageIdCounterMap.put(key, Math.min(count, scaledCount));
    }

    return Pair.of(stageIdCounterMap, tasksToBeMoved);
  }
}
