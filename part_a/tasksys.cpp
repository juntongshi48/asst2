#include "tasksys.h"
#include <thread>


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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    numThreads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::workerLoop() {
    while (true) {
        // Get next task
        int task_id = next.fetch_add(1);
        if (task_id >= num_total_tasks) {  // all tasks have been assigned
            break;
        }

        runnable->runTask(task_id, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // STATIC VERSION
    int min_tasks_per_thread;
    int N = 4; 
    int num_active_threads;
    if (num_total_tasks < N) {  // heuristic to still exploit some parallelism for very small number of tasks
        min_tasks_per_thread = 1;
    }
    else{
        min_tasks_per_thread = 4; // heuristic to avoid creating too many threads for small number of tasks
    }
    num_active_threads = std::min(
        std::min(numThreads, num_total_tasks), 
        (num_total_tasks+min_tasks_per_thread-1)/min_tasks_per_thread
    ); // avoid creating more threads than tasks

    // // Static Version with contiguous chunks
    // int num_task_per_thread = (num_total_tasks+num_active_threads-1) / num_active_threads;
    // std::vector<std::thread> threads;
    // for (int i = 0; i < num_active_threads; i++) {
    //     int start = i * num_task_per_thread;
    //     int end = std::min(start + num_task_per_thread, num_total_tasks);
    //     threads.push_back(std::thread([runnable, num_total_tasks, start, end]() {
    //         for (int j = start; j < end; j++) {
    //             runnable->runTask(j, num_total_tasks);
    //         }
    //     }));
    // }

    // // Static Version with round-robin
    // std::vector<std::thread> threads;
    // for (int i = 0; i < num_active_threads; i++) {
    //     // int start = i * num_task_per_thread;
    //     // int end = std::min(start + num_task_per_thread, num_total_tasks);
    //     threads.push_back(std::thread([runnable, num_total_tasks, num_active_threads, i]() {
    //         for (int j = i; j < num_total_tasks; j+=num_active_threads) {
    //             runnable->runTask(j, num_total_tasks);
    //         }
    //     }));
    // }

    // Dynamic Version
    std::vector<std::thread> workers;
    next.store(0);
    workers.reserve(num_active_threads);
    this->runnable = runnable;
    this-> num_total_tasks = num_total_tasks;
    for (int i = 0; i < num_active_threads-1; i++) {   // main thread also do works, so create num_threads-1 workers
        workers.emplace_back([this](){this->workerLoop();});
    }

    workerLoop(); // main thread also do works

    for (auto& t : workers) {
        t.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    
    num_threads = num_threads;
    // Create threads and start worker loops
    workers.reserve(num_threads);
    for (int i = 0; i < num_threads-1; i++) {  // main thread also do works, so create num_threads-1 workers
        workers.emplace_back([this](){this->workerLoop();});
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    stop.store(true);
    for (auto& t : workers) {
        t.join();   // wait for all workers to exit
    }
}

void TaskSystemParallelThreadPoolSpinning::workerLoop() {
    while (true) {
        if (stop.load()) {
            return;
        }
        // Wait for work
        if (!hasWork.load()) {   // spin until there is work
            std::this_thread::yield();
            continue;
        }

        // Get next task
        int task_id = next.fetch_add(1);
        if (task_id >= curr_num_total_tasks) {  // all tasks have been assigned
            std::this_thread::yield();
            continue;
        }

        curr_runnable->runTask(task_id, curr_num_total_tasks);
        finished.fetch_add(1);
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    curr_runnable=runnable;
    curr_num_total_tasks=num_total_tasks;
    next.store(0);
    finished.store(0);
    hasWork.store(true);    // turn on hasWork only after setting curr_runnable and curr_num_total_tasks

    while(true) {   // main thread also do works
        int task_id = next.fetch_add(1);
        if (task_id >= curr_num_total_tasks) {
            break;  // at this point, all tasks have been assigned, but not necessarily finished
        }
        curr_runnable->runTask(task_id, curr_num_total_tasks);
        finished.fetch_add(1);
    }
    
    while (finished.load() < curr_num_total_tasks) {
        std::this_thread::yield();  // wait for all assigned tasks to finish before returning
    }
    hasWork.store(false);
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads = num_threads;
    // Create threads and start worker loops
    workers.reserve(num_threads);
    for (int i = 0; i < num_threads-1; i++) {   // main thread also do works, so create num_threads-1 workers
        workers.emplace_back([this](){this->workerLoop();});
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    stop.store(true);
    cv_has_work.notify_all(); // wake up all sleeping workers so they can exit
    for (auto& t : workers) {
        t.join();   // wait for all workers to exit
    }
}

void TaskSystemParallelThreadPoolSleeping::workerLoop() {
    while (true) {
        // Wait for work
        std::unique_lock<std::mutex> lk(m_has_work);
        cv_has_work.wait(lk, [this]{return (hasWork.load() && next.load() < curr_num_total_tasks) || stop.load();}); // first check if there is batch of tasks to dd
        // next check if there is still tasks in the batch to do (this will sleep additional trheads in the case where the number of tasks is less than the number of threads)
        // also check stop b/c stop may be set after hasWork is set to false, so if we don't check stop here, the worker may wait forever
        if (stop.load()) {
            return;
        }
        lk.unlock();
        
        // Get next task
        int task_id = next.fetch_add(1);
        if (task_id >= curr_num_total_tasks) {  // all tasks have been assigned
            std::this_thread::yield();
            continue;
        }

        curr_runnable->runTask(task_id, curr_num_total_tasks);
        finished.fetch_add(1);
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    curr_runnable=runnable;
    curr_num_total_tasks=num_total_tasks;
    next.store(0);
    finished.store(0);
    hasWork.store(true);    // turn on hasWork only after setting curr_runnable and curr_num_total_tasks
    cv_has_work.notify_all(); // wake up all sleeping workers

    while(true) {   // main thread also do works
        int task_id = next.fetch_add(1);
        if (task_id >= curr_num_total_tasks) {
            break;  // at this point, all tasks have been assigned, but not necessarily finished
        }
        curr_runnable->runTask(task_id, curr_num_total_tasks);
        finished.fetch_add(1);
    }
    
    while (finished.load() < curr_num_total_tasks) {
        std::this_thread::yield();  // wait for all assigned tasks to finish before returning
    }
    hasWork.store(false);
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
