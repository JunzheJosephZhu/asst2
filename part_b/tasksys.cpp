#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), num_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    terminate = false;
    threads = new std::thread[num_threads];
    asleep = 0;
    for (int i=0; i<num_threads; i++) {
        threads[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::collaborate, this, i);
    }
    // printf("init done\n");
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    // printf("destructor called\n");
    mutex_.lock();
    terminate = true;
    if (asleep > 0)
        condition_variable_.notify_all();
    mutex_.unlock();
    for (int i=0; i<num_threads; i++) {
        threads[i].join();
    }
    // printf("destructor done\n");
    delete[] threads;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    std::vector<TaskID> noDeps;
    runAsyncWithDeps(runnable, num_total_tasks, noDeps);
    sync();
}

void TaskSystemParallelThreadPoolSleeping::collaborate(int thread_id) {
    IRunnable* runnable;
    int task;
    std::unique_lock<std::mutex> lk(mutex_);
    lk.unlock();
    while(true){
        // printf("terminate is %d\n", terminate);
        mutex_.lock();
        if (terminate) {
            mutex_.unlock();
            return;
        }
        // if both queues are empty, sleep
        if (queue_blocked.empty() && queue_unblocked.empty()){
            asleep++;
            condition_variable_.wait(lk);
            asleep--;
            mutex_.unlock();
            continue;

        }
        // if unblocked queue is not empty, take the first taskid, increment its counter
        if (!queue_unblocked.empty()) {
            TaskID taskid = queue_unblocked.front();
            runnable = runnables_list[taskid];
            task = taken[taskid]++;
            // if this taskid is all assigned, remove it
            if (taken[taskid] == num_total_tasks_list[taskid]) {
                queue_unblocked.erase(queue_unblocked.begin());
            }
            mutex_.unlock();
            runnable->runTask(task, num_total_tasks_list[taskid]);
            mutex_.lock();
            completed[taskid]++;
            mutex_.unlock();
        }
        // if unblocked queue is empty, push stuff from blocked queue to unblocked queue
        else{
            for (int i = 0; i < queue_blocked.size(); i++) {
                // printf("blocked queue size: %d\n", queue_blocked.size());
                TaskID taskid = queue_blocked[i];
                bool deps_cleared = true;
                for (int j = 0; j < deps_list[taskid].size(); j++) {
                    // printf("deps_list element %d: %d\n", j, deps_list[taskid][j]);
                    // printf("completed[deps_list[taskid][j]]: %d\n", completed[deps_list[taskid][j]]);
                    // printf("num_total_tasks_list[deps_list[taskid][j]]: %d\n", num_total_tasks_list[deps_list[taskid][j]]);
                    if (completed[deps_list[taskid][j]] != num_total_tasks_list[deps_list[taskid][j]]) {
                        deps_cleared = false;
                        break;
                    }
                }
                if (deps_cleared) {
                    queue_unblocked.push_back(taskid);
                    queue_blocked.erase(queue_blocked.begin() + i);
                    i--;
                }
            }
            mutex_.unlock();
        }
    }




}
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    // printf("runAsyncWithDeps is called\n");
    mutex_.lock();
    if (asleep > 0)
        condition_variable_.notify_all();
    TaskID taskid = runnables_list.size();
    runnables_list.push_back(runnable);
    deps_list.push_back(deps);
    taken.push_back(0);
    completed.push_back(0);
    num_total_tasks_list.push_back(num_total_tasks);
    queue_blocked.push_back(taskid);
    // wake up all the threads
    mutex_.unlock();
    return taskid;

}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    // printf("sync is called\n");
    while (true){
        bool all_completed = true;
        mutex_.lock();
        // can't just check queue size here. queue gets destroyed when all tasks are assigned, not completed
        for (int i = 0; i < completed.size(); i++) {
            if (completed[i] != num_total_tasks_list[i]) {
                all_completed = false;
                break;
            }
        }
        mutex_.unlock();
        if (all_completed) {
            break;
        }
    }
    // printf("sync is done\n");

    return;
}
