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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
    workers.reserve(num_threads);
    for (int i=0; i<num_threads; i++) { // main thread does not do work in this case, so we create num_threads workers
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
    cv_submitted_are_completed.notify_all(); // also wake up any sync() that might be blocked
    for (auto& t : workers) {
        t.join();   // wait for all workers to exit
    }
}

void TaskSystemParallelThreadPoolSleeping::workerLoop() {
    while (true) {
        std::shared_ptr<Launch> L;
        // Wait for work
        {
            std::unique_lock<std::mutex> lk(m);
            cv_has_work.wait(lk, [this]{return !ready_q.empty() || stop.load();}); // first check if there is a launch of tasks to do
            // also check stop b/c stop may be set after hasWork is set to false, so if we don't check stop here, the worker may wait forever
            if (stop.load()) {
                return;
            }
            L = ready_q.front();
            ready_q.pop_front();
            ready_q.push_back(L); // round-robin scheduling of launches s.t. all launches in ready_q get a chance to be worked on
        }
        
        // Get next task
        int task_id = L->next.fetch_add(1);
        if (task_id >= L->num_total_tasks) {  // all tasks have been assigned
            continue;
        }

        L->runnable->runTask(task_id, L->num_total_tasks);
        if (L->finished.fetch_add(1) == L->num_total_tasks-1) {
            on_launch_complete(L);
        }
    }
}

// Routines to do when all tasks of a launch is completed.
// Most works need to be done under lock.
void TaskSystemParallelThreadPoolSleeping::on_launch_complete(std::shared_ptr<Launch> L) {
    std::vector<TaskID> children = L->dependents;
    {   // acquire lock whenever modify ready_q and launches
        std::lock_guard<std::mutex> lk(m);
        
        // Update all_done flag
        L->all_done.store(true);
        // Remove from ready_q
        auto it = std::find(ready_q.begin(), ready_q.end(), L);
        if (it != ready_q.end()) {
            ready_q.erase(it);
        }
        // Update the global completed count
        completed_cnt++;
        
        // Notify dependents
        for (TaskID cid : children) {
            auto it = launches.find(cid);
            if (it != launches.end()) { // is a children
                std::shared_ptr<Launch> child = it->second;
                if (child->num_deps_left.fetch_sub(1) == 1) {
                    // initialize child
                    child->next.store(0);
                    child->finished.store(0);
                    child->all_done.store(false);
                    ready_q.push_back(child);
                }
            }
        }
    }
    cv_has_work.notify_all(); // new work might be available
    cv_submitted_are_completed.notify_all(); // completed_cnt has incremented, so sync() might become unblocked
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    std::vector<TaskID> empty_deps;
    runAsyncWithDeps(runnable, num_total_tasks, empty_deps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    std::shared_ptr<Launch> L = std::make_shared<Launch>();
    {   // acquire lock whenever modify ready_q and launches
        std::lock_guard<std::mutex> lk(m);
        L->tid = next_task_id++;
        L->runnable = runnable;
        L->num_total_tasks = num_total_tasks;
        L->next.store(0);
        L->finished.store(0);
        L->all_done.store(false);

        // Init dependencies
        int num_deps = 0;
        for (TaskID tid : deps) {
            auto it = launches.find(tid);
            if (it != launches.end()) { // is a dependency
                std::shared_ptr<Launch> dep_L = it->second;
                if (!dep_L->all_done.load()) {
                    num_deps++;
                    dep_L->dependents.push_back(L->tid);
                }
            }
        }
        L->num_deps_left.store(num_deps);
        launches[L->tid] = L;
        submitted_cnt++;

        if (num_deps == 0) {
            ready_q.push_back(L);
            cv_has_work.notify_all(); // new work is available
        }
    }
    return L->tid;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    std::unique_lock<std::mutex> lk(m);
    int curr_submitted_cnt = submitted_cnt; // Record the number of submited tasks at the current moment
    cv_submitted_are_completed.wait(lk, [this, curr_submitted_cnt]{ return completed_cnt >= curr_submitted_cnt; });
}
