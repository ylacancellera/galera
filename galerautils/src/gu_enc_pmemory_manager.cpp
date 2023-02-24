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
#include "gu_enc_debug.hpp"
#include "gu_enc_utils.hpp"
#include "gu_throw.hpp"

#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>


namespace gu {

/* We need at least 2 pages in the encryption cache.
   This is because memcpy is optimized to copy data
   in batches rather than byte-by-byte. It may happen that the client
   tries to memcpy >= 16 bytes at the virtual mamory pages boundary.
   In such a case we need two cache pages to be mapped to these virtual pages.
   If we had only one page, it would be not possible, and we would start
   infinite cycle of cache page map-unmap-map.
   There is one corner case when one cache page is enough. It is when the mapped
   virtual memory is the same size as the cache size.
   As the page size is a multiple of CPU page size (4k) and is rather kB than MB
   it seems not to be a big problem having always at least 2 pages. */
static const size_t CACHE_ALLOC_PAGES_MIN = 2;

/* We allow to allocate max 512 pages (allocation pages, not CPU pages).
   If cache size needs to be bigger, gcache.encryption_cache_page_size should be bigger */
static const size_t CACHE_ALLOC_PAGES_MAX = 512;

#define CLEAR_BUFFERS 0
#if CLEAR_BUFFERS
static const unsigned char FREE_PAGE_PATTERN = 0xAB;
static const unsigned char ALLOCATED_PAGE_PATTERN = 0xED;
#endif

static inline std::size_t getCpuPageSize() {
    static const std::size_t nbytes = sysconf(_SC_PAGESIZE);
    return nbytes;
}

PMemoryManager::PMemoryManager(size_t size, size_t alloc_page_size)
: create_size_(size)  // Actual size may differ because of limits
, base_(0)
, size_(0)
, free_pages_()
, my_pages_()
, fd_(-1)
, mapped_(false)
, alloc_pages_cnt_(0)
, alloc_page_size_(alloc_page_size) {
    S_DEBUG_N("+++PMemoryManager::PMemoryManager() size: %ld, alloc_page_size: %ld\n",
      size, alloc_page_size);

    // alloc_page_size has to be CPU page aligned
    if (alloc_page_size_ % getCpuPageSize() || alloc_page_size_ < getCpuPageSize()) {
        S_DEBUG_E("PMemoryManager::PMemoryManager() alloc_page_size not aligned. Requested: %ld. "
                  "Should be multiply of CPU page size %ld\n", alloc_page_size, getCpuPageSize());
        gu_throw_error(errno) << "PMemoryManager::PMemoryManager() alloc_page_size not aligned";
    }

    // how many pages do we need to satisfy size?
    alloc_pages_cnt_ = size / alloc_page_size_;
    if (size % alloc_page_size_) {
        S_DEBUG_N("PMemoryManager::PMemoryManager() adding page, size %ld is not aligned to allocation unit\n", size);
        alloc_pages_cnt_++;
    }

    alloc_pages_cnt_ = alloc_pages_cnt_ < CACHE_ALLOC_PAGES_MIN ? CACHE_ALLOC_PAGES_MIN : alloc_pages_cnt_;

    alloc_pages_cnt_ = alloc_pages_cnt_ < CACHE_ALLOC_PAGES_MAX ? alloc_pages_cnt_ : CACHE_ALLOC_PAGES_MAX;

    size_ = alloc_pages_cnt_ * alloc_page_size_;
    if (creae_tmp_file()) {
        gu_throw_error(errno) << "PMemoryManager::PMemoryManager() creation of tempfile failed";
    }
    base_ = static_cast<unsigned char*>(mmap(nullptr, size_, PROT_READ|PROT_WRITE, MAP_SHARED, fd_, 0));
    mapped_ = (base_ != MAP_FAILED);
    if (!mapped_)
    {
        gu_throw_error(errno) << "PMemoryManager::PMemoryManager() mmap() failed";
    }
    if (mlock(base_, size_)) {
        S_DEBUG_W("PMemoryManager::PMemoryManager() mlock failed. It will still work, "
                   "but swap pages into the disk, so performance will be affected\n");
    }
#if CLEAR_BUFFERS
    memset(base_, FREE_PAGE_PATTERN, size_);
#endif
    S_DEBUG_N("PMemoryManager::PMemoryManager() (x%llX - x%llX). "
              "CpuPageSize: %ld, alloc_page_size: %ld, allocPagesCnt: %ld, "
              "size requested: %ld, size allocated: %ld\n",
      ptr2ull(base_), ptr2ull(base_) + size_,
      getCpuPageSize(), alloc_page_size_, alloc_pages_cnt_, create_size_, size_);

    for (size_t i = 0; i < alloc_pages_cnt_; ++i) {
        auto page = std::make_shared<PPage>();
        page->fd_ = fd_;
        page->offset_ = i*alloc_page_size_;
        page->ptr_ = base_ + page->offset_;
        my_pages_.push_back(page);
    }
    free_pages_ = my_pages_;
    S_DEBUG_N("---PMemoryManager::PMemoryManager()\n");
}

PMemoryManager::~PMemoryManager() {
    S_DEBUG_N("+++PMemoryManager::~PMemoryManager() (x%llX - x%llX)\n",
      ptr2ull(base_), ptr2ull(base_) + size_);

    if (free_pages_.size() != alloc_pages_cnt_) {
        S_DEBUG_W("Some pages still allocated. Free pages cnt: %d\n", free_pages_.size());
    }

    if (mapped_) {
        if (munmap (base_, size_) < 0) {
            S_DEBUG_E("unmap failed");
        }
    }
    mapped_ = false;
    S_DEBUG_N("---PMemoryManager::~PMemoryManager() (x%llX - x%llX)\n",
      ptr2ull(base_), ptr2ull(base_) + size_);
}

void PMemoryManager::get_create_params(size_t* size, size_t* alloc_page_size) {
    *size = create_size_;
    *alloc_page_size = alloc_page_size_;
}


std::shared_ptr<PPage> PMemoryManager::alloc() {
    // no free pages. Need to free some pages before allocating.
    S_DEBUG_N("PMemoryManager::alloc() freePages: %d\n", free_pages_.size());
    if (free_pages_.empty()) {
        S_DEBUG_N("PMemoryManager::alloc() no free pages\n");
        return std::shared_ptr<PPage>();
    }
    auto p = free_pages_.back();
    free_pages_.pop_back();

#if CLEAR_BUFFERS
    for(size_t i = 0; i < alloc_page_size_; ++i){
        if ((unsigned char)(p->ptr_[i]) != FREE_PAGE_PATTERN) {
            S_DEBUG_E("Free page pattern does not mach\n");
            assert(0);
        }
    }
    memset(p->ptr_, ALLOCATED_PAGE_PATTERN, alloc_page_size_);
#endif
    return p;
}

void PMemoryManager::free(std::shared_ptr<PPage> page) {
#if CLEAR_BUFFERS
    memset(page->ptr_, FREE_PAGE_PATTERN, alloc_page_size_);
#endif
    free_pages_.push_back(page);
}

void PMemoryManager::free_all() {
    if(my_pages_.size() != free_pages_.size()) {
        // some pages were not released, restore clean state
        free_pages_ = my_pages_;
    }
}

bool PMemoryManager::creae_tmp_file()
{
    char path_template[] = "/tmp/XXXXXX";
    int fd (mkstemp(path_template));
    if (fd == -1) {
        return true;
    }
    // setup automatic deletion of the file
    unlink(path_template);

    // and file description in case of forking
    int flags = fcntl(fd, F_GETFD);
    if (fd == -1) {
        return true;
    }
    if (fcntl(fd, F_SETFD, flags|FD_CLOEXEC) == -1) {
        return true;
    }

    if (posix_fallocate(fd, 0, size_)) {
        S_DEBUG_E("posix_fallocate failed\n");
        return true;
    }
    fd_ = fd;
    return false;
}

}