/*
 * Copyright (C) 2010-2013 Codership Oy <info@codership.com>
 *
 * Using broadcasts instead of signals below to wake flush callers due to
 * theoretical possibility of more than 2 threads involved.
 */

#include "galera_service_thd.hpp"

const uint32_t galera::ServiceThd::A_NONE = 0;

static const uint32_t A_LAST_COMMITTED = 1U <<  0;
static const uint32_t A_RELEASE_SEQNO  = 1U <<  1;
static const uint32_t A_FLUSH          = 1U << 30;
static const uint32_t A_EXIT           = 1U << 31;

void*
galera::ServiceThd::thd_func (void* arg)
{
    galera::ServiceThd* st = reinterpret_cast<galera::ServiceThd*>(arg);
    bool exit = false;

#ifdef PXC
#ifdef HAVE_PSI_INTERFACE
    /* Register service thread to PFS */
    pfs_instr_callback(WSREP_PFS_INSTR_TYPE_THREAD,
                       WSREP_PFS_INSTR_OPS_INIT,
                       WSREP_PFS_INSTR_TAG_SERVICE_THD_THREAD,
                       NULL, NULL, NULL);
#endif /* HAVE_PSI_INTERFACE */
#endif /* PXC */

    while (!exit)
    {
        galera::ServiceThd::Data data;

        {
            gu::Lock lock(st->mtx_);

            if (A_NONE == st->data_.act_) lock.wait(st->cond_);

            data = st->data_;
            st->data_.act_ = A_NONE; // clear pending actions

            if (data.act_ & A_FLUSH)
            {
                if (A_FLUSH == data.act_)
                { // no other actions scheduled (all previous are "flushed")
                    log_info << "Service thread queue flushed.";
                    st->flush_.broadcast();
                }
                else
                { // restore flush flag for the next iteration
                    st->data_.act_ |= A_FLUSH;
                }
            }
        }

        exit = ((data.act_ & A_EXIT));

        if (!exit)
        {
            if (data.act_ & A_LAST_COMMITTED)
            {
                ssize_t const ret
                    (st->gcs_.set_last_applied(data.last_committed_));

                if (gu_unlikely(ret < 0))
                {
                    log_warn << "Failed to report last committed "
                             << data.last_committed_ << ", " << ret
                             << " (" << strerror (-ret) << ')';
                    // @todo: figure out what to do in this case
                }
                else
                {
                    log_debug << "Reported last committed: "
                              << data.last_committed_;
                }
            }

            if (data.act_ & A_RELEASE_SEQNO)
            {
                try
                {
                    st->gcache_.seqno_release(data.release_seqno_);
                }
                catch (std::exception& e)
                {
                    log_warn << "Exception releasing seqno "
                             << data.release_seqno_ << ": " << e.what();
                }
                if(data.clear_release_seqno_) {
                    data.release_seqno_ = 0;
                    data.clear_release_seqno_ = false;
                }
            }
        }
    }

#ifdef PXC
#ifdef HAVE_PSI_INTERFACE
    pfs_instr_callback(WSREP_PFS_INSTR_TYPE_THREAD,
                       WSREP_PFS_INSTR_OPS_DESTROY,
                       WSREP_PFS_INSTR_TAG_SERVICE_THD_THREAD,
                       NULL, NULL, NULL);
#endif /* HAVE_PSI_INTERFACE */
#endif /* PXC */

    return 0;
}

galera::ServiceThd::ServiceThd (GcsI& gcs, gcache::GCache& gcache) :
    gcache_ (gcache),
    gcs_    (gcs),
    thd_    (),
#ifdef PXC
#ifdef HAVE_PSI_INTERFACE
    mtx_    (WSREP_PFS_INSTR_TAG_SERVICE_THD_MUTEX),
    cond_   (WSREP_PFS_INSTR_TAG_SERVICE_THD_CONDVAR),
    flush_  (WSREP_PFS_INSTR_TAG_SERVICE_THD_FLUSH_CONDVAR),
#else
     mtx_    (),
     cond_   (),
     flush_  (),
#endif /* HAVE_PSI_INTERFACE */
#else
    mtx_    (),
    cond_   (),
    flush_  (),
#endif /* PXC */
    data_   ()
{
    gu_thread_create (&thd_, NULL, thd_func, this);
}

galera::ServiceThd::~ServiceThd ()
{
    {
        gu::Lock lock(mtx_);
        data_.act_ = A_EXIT;
        cond_.signal();
        flush_.broadcast();
    }

    gu_thread_join(thd_, NULL);
}

void
galera::ServiceThd::flush(const gu::UUID& uuid)
{
    gu::Lock lock(mtx_);

    if (!(data_.act_ & A_EXIT))
    {
        if (data_.act_ == A_NONE) cond_.signal();
        data_.act_ |= A_FLUSH;
        do { lock.wait(flush_); } while (data_.act_ & A_FLUSH);
    }

    data_.last_committed_.set(uuid);
}

void
galera::ServiceThd::reset()
{
    gu::Lock lock(mtx_);
    data_.act_ = A_NONE;
    data_.last_committed_ = gu::GTID();
}

void
galera::ServiceThd::report_last_committed(gcs_seqno_t const seqno,
                                          bool        const report)
{
    gu::Lock lock(mtx_);

    if (gu_likely(data_.last_committed_.seqno() < seqno))
    {
        data_.last_committed_.set(seqno);

        if (gu_likely(report))
        {
            if (data_.act_ == A_NONE) cond_.signal();

            data_.act_ |= A_LAST_COMMITTED;
        }
    }
}

void
galera::ServiceThd::release_seqno(gcs_seqno_t seqno, bool reset)
{
    gu::Lock lock(mtx_);

    if (data_.release_seqno_ < seqno)
    {
        data_.release_seqno_ = seqno;
        data_.clear_release_seqno_ = reset;

        if (data_.act_ == A_NONE) cond_.signal();

        data_.act_ |= A_RELEASE_SEQNO;
    }
}
