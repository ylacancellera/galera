
#pragma once

class SocketWatchdogCb
{
  public:
  virtual void operator()() = 0;

  virtual ~SocketWatchdogCb(){};
};

struct SocketWatchdogImpl;

class SocketWatchdog
{
    public:
        SocketWatchdog(SocketWatchdogCb* onExpire, unsigned int timeoutMs = 10000);

        ~SocketWatchdog();

        void start();

        void stop();

    private:

        SocketWatchdogImpl* impl_;
};

