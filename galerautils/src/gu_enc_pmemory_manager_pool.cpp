/* Copyright (c) 2022 Percona LLC and/or its affiliates. All rights
   reserved.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

#include "gu_enc_pmemory_manager.hpp"
#include "gu_enc_pmemory_manager_pool.hpp"
#include "gu_enc_debug.hpp"

namespace gu {
static uint64_t timestamp_server = 0;
static const uint64_t AGE_THREASHOLD = 10;
static const uint64_t ERASE_TRIGGER = 10;

PMemoryManagerHolder::PMemoryManagerHolder(uint64_t timestamp, std::shared_ptr<PMemoryManager> manager)
: timestamp_(timestamp)
, manager_(manager)
, mgr_size_(0)
, mgr_alloc_page_size_(0)
{
    manager_->get_create_params(&mgr_size_, &mgr_alloc_page_size_);
}

bool PMemoryManagerHolder::operator <(const PMemoryManagerHolder& rhs) const {
    return mgr_size_ < rhs.mgr_size_ && mgr_alloc_page_size_ < rhs.mgr_alloc_page_size_;
}


PMemoryManagerPool::PMemoryManagerPool(size_t manager_pool_size)
: mtx_()
, managers_()
, pool_size_max_(manager_pool_size)
, pool_size_(0) {
}

std::shared_ptr<PMemoryManager> PMemoryManagerPool::allocate(size_t alloc_page_size, size_t size) {
    std::lock_guard<std::mutex> l(mtx_);
    std::shared_ptr<PMemoryManager> result;

    S_DEBUG_N("PMemoryManagerPool::allocate(). size: %ld, page size: %ld, Pool size: %ld/%ld\n",
      size, alloc_page_size, pool_size_, pool_size_max_);
    timestamp_server++;
    bool doErase = (timestamp_server % ERASE_TRIGGER == 0);

    for (auto iter = managers_.begin(); iter != managers_.end();) {
        if (!result && iter->mgr_size_ >= size && iter->mgr_alloc_page_size_ >= alloc_page_size) {
            result = iter->manager_;
            auto erase_iter = iter;
            ++iter;
            managers_.erase(erase_iter);
            pool_size_--;
            S_DEBUG_N("Reusing PMemoryManager (size: %ld, page size: %ld\n",
              iter->mgr_size_, iter->mgr_alloc_page_size_);
        }
        if(result && !doErase) {
            break;
        }

        // once every ERASE_THREASHOLD allocations try to erase obsolete managers
        if (iter == managers_.end()) {
            break;
        }

        if (iter->timestamp_ + AGE_THREASHOLD < timestamp_server ||
            iter->timestamp_ > timestamp_server) {
            S_DEBUG_N("PMemoryManagerPool::allocate(). Removing obsolete manager."
                     " Manager timestamp: %llu, current timestamp: %llu"
                     " Manager size: %ld\n", iter->timestamp_, timestamp_server, iter->mgr_size_);
            auto erase_iter = iter;
            ++iter;
            managers_.erase(erase_iter);
        } else {
            ++iter;
        }
    }
    if (!result) {
        S_DEBUG_N("Creating new PMemoryManager\n");
        result = std::make_shared<PMemoryManager>(size, alloc_page_size);
    }
    return result;
}

void PMemoryManagerPool::free(std::shared_ptr<PMemoryManager>mgr) {
    std::lock_guard<std::mutex> l(mtx_);
    if (pool_size_ < pool_size_max_) {
        managers_.emplace(timestamp_server, mgr);
        pool_size_++;
        S_DEBUG_N("PMemoryManager returned to pool. Pool size: %ld/%ld\n", pool_size_, pool_size_max_);
    } else {
        S_DEBUG_N("PMemoryManager freed, but not to the pool. Pool size: %ld/%ld\n", pool_size_, pool_size_max_);
    }
}
}