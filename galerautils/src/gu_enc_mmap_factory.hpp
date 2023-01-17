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

#ifndef __GU_MMAP_FACTORY__
#define __GU_MMAP_FACTORY__

#include <memory>
#include "gu_mmap.hpp"

namespace gu {
class FileDescriptor;


class MMapFactory {
public:
    static std::shared_ptr<IMMap> create(FileDescriptor& fd, bool encrypt,
        size_t cache_page_size, size_t cache_size, bool sync_on_destroy, size_t unencrypted_header_size);

    MMapFactory() = delete;
};
}

#endif  /* __GU_MMAP_FACTORY__ */