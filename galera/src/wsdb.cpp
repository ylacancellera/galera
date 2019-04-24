/*
 * Copyright (C) 2010-2014 Codership Oy <info@codership.com>
 */

#include "wsdb.hpp"
#include "trx_handle.hpp"
#include "write_set.hpp"

#include "gu_lock.hpp"
#include "gu_throw.hpp"

void galera::Wsdb::print(std::ostream& os) const
{
    os << "trx map:\n";
    for (galera::Wsdb::TrxMap::const_iterator i = trx_map_.begin();
         i != trx_map_.end();
         ++i)
    {
        os << i->first << " " << *i->second << "\n";
    }
    os << "conn query map:\n";
    for (galera::Wsdb::ConnMap::const_iterator i = conn_map_.begin();
         i != conn_map_.end();
         ++i)
    {
        os << i->first << " ";
    }
    os << "\n";
}


galera::Wsdb::Wsdb()
    :
    trx_pool_  (TrxHandleMaster::LOCAL_STORAGE_SIZE(), 512, "LocalTrxHandle"),
    trx_map_   (),
#ifdef PXC
    conn_trx_map_   (),
#ifdef HAVE_PSI_INTERFACE
    trx_mutex_ (WSREP_PFS_INSTR_TAG_WSDB_TRX_MUTEX),
#else
     trx_mutex_ (),
#endif /* HAVE_PSI_INTERFACE */
#else
    trx_mutex_ (),
#endif /* PXC */
    conn_map_  (),
#ifdef PXC
#ifdef HAVE_PSI_INTERFACE
    conn_mutex_(WSREP_PFS_INSTR_TAG_WSDB_CONN_MUTEX)
#else
     conn_mutex_()
#endif /* HAVE_PSI_INTERFACE */
#else
    conn_mutex_()
#endif /* PXC */
{}


galera::Wsdb::~Wsdb()
{
    log_info << "wsdb trx map usage " << trx_map_.size()
             << " conn query map usage " << conn_map_.size();
    log_info << trx_pool_;

#ifdef PXC
    /* There is potential race when a user triggers update of wsrep_provider
    that leads to deinit/unload of the provider. deinit/unload action of
    provider waits for replication to end. stop_replication routine waits
    for any active monitors for get released. But once monitors are
    released before the connection or transaction handle is discarded
    if deinit/unload sequence try to free up/destruct the provider user may
    hit the below mentioned assert.

    In normal flow, case shouldn't arise but if the case shows-up then
    waiting for few seconds should help schedule release of connection and
    transaction handle.
    Even if wait doesn't help then it suggest some other serious issue
    that is blocking release of connection/transaction handle.
    In such case let the server assert as per the original flow.
    assert at this level should be generally safe given provider
    is unloading. */

    uint count = 5;
    while((trx_map_.size() != 0 || conn_trx_map_.size() != 0 ||
           conn_map_.size() != 0)
          && count != 0)
    {
        log_info << "giving timeslice for connection/transaction handle"
                 << " to get released";
        sleep(1);
        --count;
    }
#endif /* PXC */

#ifndef NDEBUG
    log_info << *this;
    assert(trx_map_.size() == 0);
    assert(conn_map_.size() == 0);
#endif // !NDEBUG
}

inline galera::TrxHandleMasterPtr
galera::Wsdb::create_trx(const TrxHandleMaster::Params& params,
                         const wsrep_uuid_t&            source_id,
                         wsrep_trx_id_t const           trx_id)
{
#ifdef PXC
    TrxHandleMasterPtr trx(new_trx(params, source_id, trx_id));

    if (trx_id == wsrep_trx_id_t(-1))
    {
        /* trx_id is default so add trx object to connection map
        that is maintained based on pthread_id (alias for connection_id). */
        std::pair<ConnTrxMap::iterator, bool> i
             (conn_trx_map_.insert(std::make_pair(pthread_self(), trx)));
        if (gu_unlikely(i.second == false)) gu_throw_fatal;

        return i.first->second;
    }

    std::pair<TrxMap::iterator, bool> i (trx_map_.insert(std::make_pair(trx_id, trx)));
    if (gu_unlikely(i.second == false)) gu_throw_fatal;

    return i.first->second;
#else
    TrxHandleMasterPtr trx(new_trx(params, source_id, trx_id));

    std::pair<TrxMap::iterator, bool> i (trx_map_.insert(std::make_pair(trx_id, trx)));
    if (gu_unlikely(i.second == false)) gu_throw_fatal;

    return i.first->second;
#endif /* PXC */

}


galera::TrxHandleMasterPtr
galera::Wsdb::get_trx(const TrxHandleMaster::Params& params,
                      const wsrep_uuid_t&            source_id,
                      wsrep_trx_id_t const           trx_id,
                      bool const                     create)
{
#ifdef PXC
    gu::Lock lock(trx_mutex_);

    if (trx_id == wsrep_trx_id_t(-1))
    {
        /* trx_id is default (-1) search in conn_trx_map using pthread-id */
        pthread_t const id = pthread_self();
        TrxMap::iterator const i(conn_trx_map_.find(id));
        if (i == conn_trx_map_.end() && create)
        {
            return create_trx(params, source_id, trx_id);
        }
        else if (i == conn_trx_map_.end())
        {
            return TrxHandleMasterPtr();
        }

        return i->second;
    }

    TrxMap::iterator const i(trx_map_.find(trx_id));
    if (i == trx_map_.end() && create)
    {
        return create_trx(params, source_id, trx_id);
    }
     else if (i == trx_map_.end())
    {
        return TrxHandleMasterPtr();
    }

    return i->second;
#else
    gu::Lock lock(trx_mutex_);
    TrxMap::iterator const i(trx_map_.find(trx_id));
    if (i == trx_map_.end() && create)
    {
        return create_trx(params, source_id, trx_id);
    }
    else if (i == trx_map_.end())
    {
        return TrxHandleMasterPtr();
    }

    return i->second;
#endif /* PXC */
}


galera::Wsdb::Conn*
galera::Wsdb::get_conn(wsrep_conn_id_t const conn_id, bool const create)
{
    gu::Lock lock(conn_mutex_);

    ConnMap::iterator i(conn_map_.find(conn_id));

    if (conn_map_.end() == i)
    {
        if (create == true)
        {
            std::pair<ConnMap::iterator, bool> p
                (conn_map_.insert(std::make_pair(conn_id, Conn(conn_id))));

            if (gu_unlikely(p.second == false)) gu_throw_fatal;

            return &p.first->second;
        }

        return 0;
    }

    return &(i->second);
}


galera::TrxHandleMasterPtr
galera::Wsdb::get_conn_query(const TrxHandleMaster::Params& params,
                             const wsrep_uuid_t&            source_id,
                             wsrep_conn_id_t const          conn_id,
                             bool const                     create)
{
    Conn* const conn(get_conn(conn_id, create));

    if (0 == conn)
    {
        throw gu::NotFound();
    }

    if (conn->get_trx() == 0 && create == true)
    {
        TrxHandleMasterPtr trx
            (TrxHandleMaster::New(trx_pool_, params, source_id, conn_id, -1),
             TrxHandleMasterDeleter());
        conn->assign_trx(trx);
    }

    return conn->get_trx();
}


void galera::Wsdb::discard_trx(wsrep_trx_id_t trx_id)
{
#ifdef PXC
    gu::Lock lock(trx_mutex_);
    if (trx_id == wsrep_trx_id_t(-1))
    {
        ConnTrxMap::iterator i;
        if ((i = conn_trx_map_.find(pthread_self())) != conn_trx_map_.end())
        {
            conn_trx_map_.erase(i);
        }
    }

    TrxMap::iterator i;
    if ((i = trx_map_.find(trx_id)) != trx_map_.end())
    {
        trx_map_.erase(i);
    }
#else
    gu::Lock lock(trx_mutex_);
    TrxMap::iterator i;
    if ((i = trx_map_.find(trx_id)) != trx_map_.end())
    {
        trx_map_.erase(i);
    }
#endif /* PXC */
}


void galera::Wsdb::discard_conn_query(wsrep_conn_id_t conn_id)
{
    gu::Lock lock(conn_mutex_);
    ConnMap::iterator i;
    if ((i = conn_map_.find(conn_id)) != conn_map_.end())
    {
        i->second.reset_trx();
        conn_map_.erase(i);
    }
}
