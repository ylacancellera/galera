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

#ifndef __GU_ENCRYPTION__
#define __GU_ENCRYPTION__

#include <string>
#include <functional>

namespace gu {
class UUID;
class Config;

#define ptr2ull(ptr) ((unsigned long long)ptr)

std::string encode64(const std::string& binary);
std::string decode64(const std::string& base64);
std::string generate_random_key();
std::string encrypt_key(const std::string &key_to_be_encrypted, const std::string &key);
std::string decrypt_key(const std::string &key_to_be_decrypted, const std::string &key);
std::string create_master_key_name(const UUID& const_uuid, const UUID& uuid, int keyId);

class MasterKeyProvider {
public:
    MasterKeyProvider(std::function<std::string(const std::string&)> get_key_cb,
      std::function<bool(const std::string&)> create_key_cb);
    MasterKeyProvider(const MasterKeyProvider&) = delete;
    MasterKeyProvider& operator=(const MasterKeyProvider&) = delete;

    void register_key_rotation_request_observer(std::function<bool()> fn);
    bool notify_key_rotation_observer();
    std::string get_key(const std::string& keyId);
    bool create_key(const std::string& keyId);


private:
    std::function<bool()> key_rotation_observer;
    std::function<std::string(const std::string&)> get_key_cb_;
    std::function<bool(const std::string&)> create_key_cb_;
};

}
#endif  /* __GU_ENCRYPTION__ */