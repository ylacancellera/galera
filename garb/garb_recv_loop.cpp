/* Copyright (C) 2011-2020 Codership Oy <info@codership.com> */

#include "garb_recv_loop.hpp"

#include <signal.h>
#include <gu_thread.hpp>
#include "process.h"
#include "gu_atomic.h"

namespace garb
{

static Gcs*
global_gcs(0);

void
signal_handler (int signum)
{
    log_info << "Received signal " << signum;
    global_gcs->close();
}


RecvLoop::RecvLoop (const Config& config)
    :
    config_(config),
    gconf_ (),
    params_(gconf_),
    parse_ (gconf_, config_.options()),
    gcs_   (gconf_, config_.name(), config_.address(), config_.group()),
    rcode_ (0)
{
    /* set up signal handlers */
    global_gcs = &gcs_;

    struct sigaction sa;

    memset (&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = signal_handler;

    if (sigaction (SIGTERM, &sa, NULL))
    {
        gu_throw_error(errno) << "Failed to install signal handler for "
                              << "SIGTERM";
    }

    if (sigaction (SIGINT, &sa, NULL))
    {
        gu_throw_error(errno) << "Failed to install signal handler for "
                              << "SIGINT";
    }

    rcode_ = loop();
}

void* pipe_to_log(void* pipe) {
    const int out_len = 1024;
    char out_buf[out_len];
    char* p;
    while ((p = fgets(out_buf, out_len, static_cast<FILE*>(pipe))) != NULL) {
        log_info << "[SST script] " << out_buf;
    }
    return NULL;
}

struct err_log_args {
    FILE* pipe;
    bool* sst_ended;
    Gcs* gcs;
};

void* err_log(void* arg)
{
    err_log_args* args(static_cast<err_log_args*>(arg));

    pipe_to_log(args->pipe);
    log_info << "SST script ended";
    gu_atomic_set_n(args->sst_ended, true);
    args->gcs->close();
    return NULL;
}

struct status_args {
    Gcs* gcs;
    ssize_t sst_source;
    process* proc;
};

void* status(void* arg)
{
    status_args* args(static_cast<status_args*>(arg));

    while (true) {
        gcs_node_state_t st = args->gcs->state_for(args->sst_source);
        if(st != GCS_NODE_STATE_DONOR) {
            // The donor is going back to SYNCED. If SST streaming didn't start yet,
            // it won't.
            // Send SIGINT to the script and let it handle this situation.
            args->proc->interrupt();
            break;
        }
        sleep(1);
    }
    return NULL;
}

int
RecvLoop::loop()
{
    process p(config_.recv_script().c_str(), "rw", NULL, false);
    gu_thread_t sst_out_log_thread;
    gu_thread_t sst_err_log_thread;
    gu_thread_t sst_status_thread;

    err_log_args err_args;
    err_args.gcs = &gcs_;
    status_args st_args;
    st_args.proc = &p;
    st_args.gcs = &gcs_;

    bool sst_ended = false;
    ssize_t sst_source = 0;

    while (1)
    {
        gcs_action act;

        gcs_.recv (act);

        switch (act.type)
        {
        case GCS_ACT_TORDERED:
            if (gu_unlikely(!(act.seqno_g & 127)))
                /* == report_interval_ of 128 */
            {
                gcs_.set_last_applied (act.seqno_g);
            }
            break;
        case GCS_ACT_COMMIT_CUT:
            break;
        case GCS_ACT_STATE_REQ:
            gcs_.join (-ENOSYS); /* we can't donate state */
            break;
        case GCS_ACT_CONF:
        {
            const gcs_act_conf_t* const cc
                (reinterpret_cast<const gcs_act_conf_t*>(act.buf));

            if (cc->conf_id > 0) /* PC */
            {
                if (GCS_NODE_STATE_PRIM == cc->my_state)
                {
                    sst_source = gcs_.request_state_transfer (config_.sst(),config_.donor());
                    if(config_.recv_script().empty()) {
                        gcs_.join(cc->seqno);
                    } else {
                        log_info << "Starting SST script";
                        p.execute("rw", NULL);

                        err_args.pipe = p.err_pipe();
                        err_args.sst_ended = &sst_ended;
                        gu_thread_create(&sst_err_log_thread, NULL, err_log, &err_args);
                        gu_thread_create(&sst_out_log_thread, NULL, pipe_to_log, p.pipe());
                        st_args.sst_source = sst_source;
                        gu_thread_create(&sst_status_thread, NULL, status, &st_args);
                    }
                }
            }
            else if (cc->memb_num == 0) // SELF-LEAVE after closing connection
            {
                if(!config_.recv_script().empty()) {
                    if(gu_atomic_get_n(&sst_ended)) {
                        // Good path: we decided to close the connection after the receiver script closed its
                        // standard output. We wait for it to exit and return its error code.
                        log_info << "Waiting for SST script to stop";
                        const int ret = p.wait();
                        log_info << "SST script stopped";
                        gu_thread_join(sst_err_log_thread, NULL);
                        gu_thread_join(sst_out_log_thread, NULL);
                        gu_thread_cancel(sst_status_thread);
                        gu_thread_join(sst_status_thread, NULL);
                        log_info << "Exiting main loop";
                        return ret;
                    } else {
                        // Error path: we are closing the connection because there is an SST error,
                        // such as a non existent donor side SST script was specified
                        // As the receiver side script is already running, and is most likely waiting for a TCP
                        // connection, we terminate it and report an error.
                        log_info << "Terminating SST script";
                        p.terminate();
                        gu_thread_join(sst_err_log_thread, NULL);
                        gu_thread_join(sst_out_log_thread, NULL);
                        gu_thread_cancel(sst_status_thread);
                        gu_thread_join(sst_status_thread, NULL);
                        log_info << "Exiting main loop";
                        return 1;
                    }
                } else {
                        log_info << "Exiting main loop";
                        return 0;
                }
            }

            if (config_.sst() != Config::DEFAULT_SST)
            {
                // we requested custom SST, so we're done here
                if(config_.recv_script().empty()) {
                    gcs_.close();
                }
            }

            break;
        }
        case GCS_ACT_INCONSISTENCY:
            // something went terribly wrong, restart needed
            gcs_.close();
            return 0;
        case GCS_ACT_JOIN:
        case GCS_ACT_SYNC:
        case GCS_ACT_FLOW:
        case GCS_ACT_SERVICE:
        case GCS_ACT_ERROR:
        case GCS_ACT_UNKNOWN:
            break;
        }

        if (act.buf)
        {
            free (const_cast<void*>(act.buf));
        }
    }
    return 0;
}

} /* namespace garb */
