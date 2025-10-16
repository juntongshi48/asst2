#include "tasksys.h"
#include <thread>

#define MAXCHUNK 4 // default chunk size for task stealing

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
    numThreads = num_threads;
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

// bool TaskSystemParallelThreadPoolSleeping::hasUnclaimedReady(){
//     for (auto& L : ready_q) {
//         if (L->next.load() < L->num_total_tasks) {
//             return true;
//         }
//     }
//     return false;
// };

void TaskSystemParallelThreadPoolSleeping::workerLoop() {
    while (true) {
        std::shared_ptr<Launch> L;
        // Wait for work
        {
            std::unique_lock<std::mutex> lk(m);
            cv_has_work.wait(lk, [this]{return !ready_q.empty() || stop.load();}); // first check if there is a launch of tasks to do
            // hasUunclaimedReady() provides a more rigourous check than just checking if ready_q is non-empty, as it also check if all tasks has been claimed
            // also check stop b/c stop may be set after hasWork is set to false, so if we don't check stop here, the worker may wait forever
            if (stop.load()) {
                return;
            }
            L = ready_q.front();
            int len = ready_q.size();
            if (len > 1) {  // round-robin scheduling of launches s.t. all launches in ready_q get a chance to be worked on
                // this improved mathoperation+treereduction test
                ready_q.pop_front();
                ready_q.push_back(L);
            }
        }
        
        // // Single task at a time
        // int task_id = L->next.fetch_add(1);
        // if (task_id >= L->num_total_tasks) {  // all tasks have been assigned
        //     continue;
        // }

        // L->runnable->runTask(task_id, L->num_total_tasks);
        // if (L->finished.fetch_add(1) == L->num_total_tasks-1) {
        //     on_launch_complete(L);
        // }
        
        // Chunk of tasks at a time
        while(true){
            int start = L->next.fetch_add(L->chunk_size);
            if (start >= L->num_total_tasks) break;

            int end = std::min(start + L->chunk_size, L->num_total_tasks);
            for (int task_id = start; task_id < end; ++task_id)
                L->runnable->runTask(task_id, L->num_total_tasks);

            int num_finished = end - start;
            if (L->finished.fetch_add(num_finished) == L->num_total_tasks-num_finished)
            {
                on_launch_complete(L);
                break;
            }
        }
    }
}

// Routines to do when all tasks of a launch is completed.
// Most works need to be done under lock.
void TaskSystemParallelThreadPoolSleeping::on_launch_complete(std::shared_ptr<Launch> L) {
    std::vector<TaskID> children;
    int num_new_works=0;
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
        
        // Take the snap of children in side the lock
        children = L->dependents;
        // Remove from launches
        launches.erase(L->tid);
        
        // Notify dependents
        for (TaskID cid : children) {
            auto it = launches.find(cid);
            if (it != launches.end()) { // is a children
                std::shared_ptr<Launch> child = it->second;
                if (child->num_deps_left.fetch_sub(1) == 1) {   // if child has no more deps
                    // initialize child
                    child->next.store(0);
                    child->finished.store(0);
                    child->all_done.store(false);
                    ready_q.push_back(child);
                    num_new_works++;
                }
            }
        }
    }
    if (num_new_works > 0){
        cv_has_work.notify_all(); // new work might be available
    }
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

    // Initialize chunk_size
    // It should not be to large, otherwise the load balancing will be poor(e.g.ping_pong_unequal)
    // It should not be too small, otherwise the overhead of very tiny tasks(e.g. super_super_light) will be large
    int chunk_size=std::min(MAXCHUNK, std::max((num_total_tasks + numThreads - 1)/numThreads-2, 1));

    // Initialized the number of active threads
    int min_tasks_per_thread;
    int num_active_threads;
    int N = 10; 
    if (num_total_tasks <= N) {
        min_tasks_per_thread = 1;
    } // heuristic to still exploit some parallelism for very small number of tasks
    else{
        min_tasks_per_thread = 2;
    }
    // else {
    //     min_tasks_per_thread = 8;
    // } // heuristic to avoid creating too many threads for small number of tasks
    num_active_threads = std::min(
        std::min(numThreads, num_total_tasks), 
        (num_total_tasks+min_tasks_per_thread-1)/min_tasks_per_thread
    ); // avoid creating more threads than tasks

    std::shared_ptr<Launch> L = std::make_shared<Launch>();
    {   // acquire lock whenever modify ready_q and launches
        std::lock_guard<std::mutex> lk(m);
        L->tid = next_task_id++;
        L->runnable = runnable;
        L->num_total_tasks = num_total_tasks;
        L->next.store(0);
        L->finished.store(0);
        L->all_done.store(false);
        L->chunk_size = chunk_size;

        // Init dependencies
        int num_deps = 0;
        for (TaskID tid : deps) {
            auto it = launches.find(tid);
            if (it != launches.end()) { // this parent is still active
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
            if (num_active_threads < numThreads) {
                for (int i=0; i<num_active_threads; i++) {
                    cv_has_work.notify_one(); //cheaper
                }
            }
            else{
                cv_has_work.notify_all(); //realatively expensitve
            }
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
