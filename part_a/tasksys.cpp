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
    ready = new bool[num_threads];
    for (int i=0; i<num_threads; i++) {
        ready[i] = true;
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
        } else {
            need_run = false;
            ready[thread_id] = true;
        }
        mutex_.unlock();
        if (need_run) {
            runnable->runTask(task, num_total_tasks);
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
    this -> current_task = 0;
    this -> num_total_tasks = num_total_tasks;
    // in case threads can't set ready in time
    for (int i=0; i<num_threads; i++) {
        ready[i] = false;
    }
    mutex_.unlock();

    // check all is done
    while (true){
        bool all_done = true;
        for (int i=0; i<num_threads; i++) {
            if (!ready[i]) {
                all_done = false;
                break;
            }
        }
        if (all_done) {
            break;
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
    quit = 0;
    completed = new bool[num_threads];
    for (int i=0; i<num_threads; i++){
        completed[i] = false;
    }
    quit = new bool[num_threads];
    for (int i=0; i<num_threads; i++){
        quit[i] = false;
    }
    threads = new std::thread[num_threads];
    for (int i=0; i<num_threads; i++) {
        threads[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::collaborate, this, i);
    }
}

void TaskSystemParallelThreadPoolSleeping::collaborate(int thread_id) {
    // acquire lock 
    std::unique_lock<std::mutex> lk(mutex_);
    int task;
    while (true) {
        if (num_total_tasks == -1){ // quit if total task is -1
            // printf("thread %d quit\n", thread_id);
            lk.unlock();
            quit[thread_id] = true;
            return;
        }
        if (current_task < num_total_tasks){ // some work to do
            task = current_task++;
            // unlock. Do work. Lock again
            lk.unlock();
            runnable->runTask(task, num_total_tasks);
            lk.lock();
        }
        else{ // no work to do. wake master
            completed[thread_id] = true;
            condition_variable_.wait(lk);
        }
    }
}


void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    printf("run\n");
    this -> runnable = runnable;
    mutex_.lock();
    current_task = 0;
    for (int i=0; i<num_threads; i++){
        completed[i] = false;
    }
    this -> num_total_tasks = num_total_tasks;
    mutex_.unlock();

    while(current_task < num_total_tasks){
        condition_variable_.notify_all();
    }
    bool all_completed = false;
    while(!all_completed){
        all_completed = true;
        for (int i=0; i<num_threads; i++){
            if (!completed[i]){
                all_completed = false;
                break;
            }
        }
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
    bool all_quit = false;
    while(!all_quit){
        condition_variable_.notify_all();
        all_quit = true;
        for (int i=0; i<num_threads; i++){
            if (!quit[i]){
                all_quit = false;
                break;
            }
        }
    }
    for (int i=0; i<num_threads; i++)
        threads[i].join();
    delete[] threads;
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