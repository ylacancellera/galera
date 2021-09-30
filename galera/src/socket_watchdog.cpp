
#include <thread>
#include <mutex>
#include <condition_variable>
#include "socket_watchdog.hpp"

struct SocketWatchdogImpl {
  SocketWatchdogCb*       eventCbFn_;
  bool                    active_;
  bool                    alive_;
  bool                    restart_;
  int                     expire_cnt_;
  std::mutex              mtx_;
  std::condition_variable cv_;

  std::thread t_;

  SocketWatchdogImpl(SocketWatchdogCb* onExpire, unsigned int timeoutMs= 10000)
      : eventCbFn_(onExpire),
        active_(false),
        alive_(true),
        restart_(true),
        expire_cnt_(timeoutMs / 10),
        mtx_(),
        cv_(),
        t_([this]() {
          bool aliveSnapshot= alive_;

          while (aliveSnapshot)
          {
            bool activeSnapshot;
            bool restartSnapshot= false;
            int  counter;

            // Wait for the trigger. (start, stop or destructor).
            // Once triggered, collect current state of control flags.
            {
              std::unique_lock<std::mutex> lock(mtx_);
              while (!active_)
                cv_.wait(lock);
              activeSnapshot= active_;
              aliveSnapshot= alive_;
              counter= expire_cnt_;

              // Here we do not capture restart_ because we just set up
              // fresh state of the watchdog.
              restart_= false;
            }

            // Timer loop.
            while (activeSnapshot && aliveSnapshot && !restartSnapshot)
            {
              if (counter == 0)
              {
                // Timeout expired. Call registered delegate and
                // deactivate the watchdog.
                (*eventCbFn_)();

                std::unique_lock<std::mutex> lock(mtx_);
                active_= false;
                break;
              }

              {
                // Watit for 10ms, than collect current state of
                // control flags.
                std::unique_lock<std::mutex> lock(mtx_);
                cv_.wait_for(lock, std::chrono::milliseconds(10));
                activeSnapshot= active_;
                aliveSnapshot= alive_;
                // If in the meantime, when we were not under lock,
                // stop-start sequence was called, it means we need
                // to restart the timer loop.
                restartSnapshot= restart_;
              }
              --counter;
            }
          }
        })
  {
  }

  void start()
  {
    std::unique_lock<std::mutex> lock(mtx_);
    active_= true;
    // Inform executor thread that watchdog was just started
    // and it is necessary to restart timer loop.
    restart_= true;
    cv_.notify_one();
  }

  void stop()
  {
    std::unique_lock<std::mutex> lock(mtx_);
    active_= false;
    cv_.notify_one();
  }


  ~SocketWatchdogImpl()
  {
    {
      std::unique_lock<std::mutex> lock(mtx_);
      alive_= false;
      active_= true;
      cv_.notify_one();
    }
    t_.join();
  }

};


SocketWatchdog::SocketWatchdog(SocketWatchdogCb* onExpire,
                               unsigned int     timeoutMs)
    : impl_(new SocketWatchdogImpl(onExpire, timeoutMs))
{
}

SocketWatchdog::~SocketWatchdog()
{
  delete impl_;
}

void SocketWatchdog::start()
{
  impl_->start();
}

void SocketWatchdog::stop()
{
  impl_->stop();
}
