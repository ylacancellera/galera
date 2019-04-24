//
// Copyright (C) 2010-2014 Codership Oy <info@codership.com>
//
#ifndef GALERA_WSDB_HPP
#define GALERA_WSDB_HPP

#include "trx_handle.hpp"
#include "wsrep_api.h"
#include "gu_unordered.hpp"

namespace galera
{
    class Wsdb
    {

        class Conn
        {
        public:
            Conn(wsrep_conn_id_t conn_id)
                :
                conn_id_(conn_id),
                trx_()
            { }

            Conn(const Conn& other)
                :
                conn_id_(other.conn_id_),
                trx_(other.trx_)
            { }

            ~Conn() { }

            void assign_trx(TrxHandleMasterPtr trx)
            {
                trx_ = trx;
            }

            void reset_trx()
            {
                trx_ = TrxHandleMasterPtr();
            }

            TrxHandleMasterPtr get_trx()
            {
                return trx_;
            }

        private:
            void operator=(const Conn&);
            wsrep_conn_id_t conn_id_;
            TrxHandleMasterPtr trx_;
        };

        class TrxHash
        {
        public:
            size_t operator()(const wsrep_trx_id_t& key) const { return key; }
        };

        typedef gu::UnorderedMap<wsrep_trx_id_t, TrxHandleMasterPtr, TrxHash>
        TrxMap;

#ifdef PXC
        /* TrxMap structure doesn't take into consideration presence of 2 trx
        objects with same trx_id (2^64 - 1 which is default trx_id) belonging
        to 2 different connections.
        This eventually causes same trx object to get shared among 2 different
        unrelated connections which causes state in-consistency leading
        to crash. (RACE CONDITION).
        This problem could be solved by taking into consideration conn-id but
        that would invite interface change to avoid this we maintain a separate
        map of such trx object based on pthread_id. */
        class ConnTrxHash
        {
        public:
            size_t operator()(const pthread_t& key) const { return key; }
        };

        typedef gu::UnorderedMap<pthread_t, TrxHandleMasterPtr, ConnTrxHash>
        ConnTrxMap;
#endif /* PXC */

        class ConnHash
        {
        public:
            size_t operator()(const wsrep_conn_id_t& key) const { return key; }
        };

        typedef gu::UnorderedMap<wsrep_conn_id_t, Conn, ConnHash> ConnMap;

    public:

        TrxHandleMasterPtr get_trx(const TrxHandleMaster::Params& params,
                                   const wsrep_uuid_t&            source_id,
                                   wsrep_trx_id_t                 trx_id,
                                   bool                           create =false);

        TrxHandleMasterPtr new_trx(const TrxHandleMaster::Params& params,
                                   const wsrep_uuid_t&            source_id,
                                   wsrep_trx_id_t                 trx_id)
        {
            return TrxHandleMasterPtr(TrxHandleMaster::New(trx_pool_, params,
                                                           source_id, -1, trx_id),
                                      TrxHandleMasterDeleter());
        }

        void discard_trx(wsrep_trx_id_t trx_id);

        TrxHandleMasterPtr get_conn_query(const TrxHandleMaster::Params&,
                                          const wsrep_uuid_t&,
                                          wsrep_conn_id_t conn_id,
                                          bool create = false);

        void discard_conn_query(wsrep_conn_id_t conn_id);

        Wsdb();
        ~Wsdb();

        void print(std::ostream& os) const;

        struct stats
        {
            stats(size_t n_trx, size_t n_conn)
                : n_trx_(n_trx)
                , n_conn_(n_conn)
            { }
            size_t n_trx_;
            size_t n_conn_;
        };

        stats get_stats() const
        {
            gu::Lock trx_lock(trx_mutex_);
            gu::Lock conn_lock(conn_mutex_);
            stats ret(trx_map_.size(), conn_map_.size());
            return ret;
        }

    private:
        // Create new trx handle
        TrxHandleMasterPtr create_trx(const TrxHandleMaster::Params& params,
                                      const wsrep_uuid_t&            source_id,
                                      wsrep_trx_id_t                 trx_id);

        Conn*      get_conn(wsrep_conn_id_t conn_id, bool create);

        static const size_t trx_mem_limit_ = 1 << 20;

        TrxHandleMaster::Pool trx_pool_;

        TrxMap       trx_map_;
#ifdef PXC
        ConnTrxMap   conn_trx_map_;
#ifdef HAVE_PSI_INTERFACE
        gu::MutexWithPFS
                     trx_mutex_;
#else
         gu::Mutex    trx_mutex_;
#endif /* HAVE_PSI_INTERFACE */
#else
        gu::Mutex    trx_mutex_;
#endif /* PXC */
        ConnMap      conn_map_;
#ifdef PXC
#ifdef HAVE_PSI_INTERFACE
        gu::MutexWithPFS
                     conn_mutex_;
#else
         gu::Mutex    conn_mutex_;
#endif /* HAVE_PSI_INTERFACE */
#else
        gu::Mutex    conn_mutex_;
#endif /* PXC */
    };

    inline std::ostream& operator<<(std::ostream& os, const Wsdb& w)
    {
        w.print(os); return os;
    }
}


#endif // GALERA_WSDB_HPP
