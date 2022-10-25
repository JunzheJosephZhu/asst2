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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), num_threads(num_threads), current_task(0), mutex_() {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::collaborate(IRunnable* runnable, int num_total_tasks) {
    while (true){
        mutex_.lock();
        int task = current_task++;
        mutex_.unlock();
        if (task >= num_total_tasks) {
            break;
        }
        runnable->runTask(task, num_total_tasks);
    }
    return;
}
void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::thread threads[this->num_threads];
    for (int i=0; i<this->num_threads; i++)
        threads[i] = std::thread(&TaskSystemParallelSpawn::collaborate, this, runnable, num_total_tasks);
    
    for (int i=0; i<this->num_threads; i++)
        threads[i].join();
    current_task = 0;
    return;
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), num_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    current_task = 0;
    num_total_tasks = 0;
    threads = new std::thread[num_threads];
    ready = new std::atomic<bool>[num_threads];
    for (int i=0; i<num_threads; i++) {
        ready[i] = false;
    }
    for (int i=0; i<num_threads; i++) {
        threads[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::collaborate, this, i);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    num_total_tasks = -1;
    for (int i=0; i<num_threads; i++)
        threads[i].join();
    delete[] threads;
    delete[] ready;
}

void TaskSystemParallelThreadPoolSpinning::collaborate(int thread_id) {
    bool need_run;
    int task;
    while (true) {
        // quit if total task is -1
        if (num_total_tasks == -1)
            return;
        // check if something needs to be done
        mutex_.lock();
        if (current_task < num_total_tasks){
            task = current_task++;
            need_run = true;
            mutex_.unlock();
            runnable->runTask(task, num_total_tasks);
        } else {
            need_run = false;
            ready[thread_id] = true;
            mutex_.unlock();
        }

    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    this -> runnable = runnable;
    mutex_.lock();
    current_task = 0;
    this -> num_total_tasks = num_total_tasks;
    // in case threads can't set ready in time
    for (int i=0; i<num_threads; i++) {
        ready[i] = false;
    }
    mutex_.unlock();

    bool all_done = false;
    while(!all_done){
        all_done = true;
        for (int i=0; i<num_threads; i++) {
            if (!ready[i]) {
                all_done = false;
            }
        }
    }

}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
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
    current_task = 0;
    num_total_tasks = 0;
    threads = new std::thread[num_threads];
    ready = new std::atomic<bool>[num_threads];
    for (int i=0; i<num_threads; i++) {
        ready[i] = false;
    }
    for (int i=0; i<num_threads; i++) {
        threads[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::collaborate, this, i);
    }
    // wait till all threads sleep
    bool all_done = false;
    while(!all_done){
        mutex_.lock();
        all_done = true;
        for (int i=0; i<num_threads; i++) {
            if (!ready[i]) {
                all_done = false;
            }
        }
        mutex_.unlock();
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_total_tasks = -1;
    condition_variable_.notify_all();
    for (int i=0; i<num_threads; i++)
        threads[i].join();
    delete[] threads;
    delete[] ready;
}

void TaskSystemParallelThreadPoolSleeping::collaborate(int thread_id) {
    bool need_run;
    int task;
    std::unique_lock<std::mutex> lk(mutex_);
    lk.unlock();
    while (true) {
        // quit if total task is -1
        if (num_total_tasks == -1)
            return;
        // check if something needs to be done
        mutex_.lock();
        if (current_task < num_total_tasks){
            task = current_task++;
            need_run = true;
            mutex_.unlock();
            runnable->runTask(task, num_total_tasks);
        } else {
            need_run = false;
            ready[thread_id] = true;
            master_mutex_.lock();
            master_condition_variable_.notify_one();
            master_mutex_.unlock();
            condition_variable_.wait(lk);
            mutex_.unlock();
        }

    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    this -> runnable = runnable;
    mutex_.lock();
    current_task = 0;
    this -> num_total_tasks = num_total_tasks;
    // in case threads can't set ready in time
    for (int i=0; i<num_threads; i++) {
        ready[i] = false;
    }
    mutex_.unlock();
    // wake all threads
    condition_variable_.notify_all();


    mutex_.lock();
    // are all threads ready?
    bool any_done = false;
    for (int i=0; i<num_threads; i++) {
        if (ready[i]) {
            any_done = true;
        }
    }
    if (!any_done) { // if no threads is ready, block them from sending notification until master sleeps
        std::unique_lock<std::mutex> lk(master_mutex_);
        mutex_.unlock();
        master_condition_variable_.wait(lk);
        lk.unlock();
    }
    else{ // if any thread is ready, no need to wait
        mutex_.unlock();
    }

    bool all_done = false;
    while(!all_done){
        mutex_.lock();
        all_done = true;
        for (int i=0; i<num_threads; i++) {
            if (!ready[i]) {
                all_done = false;
            }
        }
        mutex_.unlock();
    }



}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
