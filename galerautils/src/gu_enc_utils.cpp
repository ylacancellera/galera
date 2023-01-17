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

#include "gu_enc_utils.hpp"
#include "gu_logger.hpp"
#include "gu_uuid.hpp"
#include "gu_assert.hpp"
#include "gu_config.hpp"
#include "enc_stream_cipher.h"

#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <boost/algorithm/string.hpp>
#include <openssl/rand.h>
#include <openssl/err.h>

// Base64 inspired by
// https://stackoverflow.com/questions/7053538/how-do-i-encode-a-string-to-base64-using-only-boost
namespace gu {

std::string encode64(const std::string& binary)
{
    using namespace boost::archive::iterators;
    using It = base64_from_binary<transform_width<std::string::const_iterator, 6, 8>>;
    auto base64 = std::string(It(binary.begin()), It(binary.end()));
    // Add padding.
    return base64.append((3 - binary.size() % 3) % 3, '=');
}

std::string decode64(const std::string& base64)
{
    using namespace boost::archive::iterators;
    using It = transform_width<binary_from_base64<std::string::const_iterator>, 8, 6>;
    auto binary = std::string(It(base64.begin()), It(base64.end()));
    // Remove padding.
    auto length = base64.size();
    if(binary.size() > 2 && base64[length - 1] == '=' && base64[length - 2] == '=')
    {
        binary.erase(binary.end() - 2, binary.end());
    }
    else if(binary.size() > 1 && base64[length - 1] == '=')
    {
        binary.erase(binary.end() - 1, binary.end());
    }
    return binary;
}

std::string generate_random_key() {
    char buf[Aes_ctr::FILE_KEY_LENGTH];
    int rc = RAND_bytes(reinterpret_cast<unsigned char*>(buf), Aes_ctr::FILE_KEY_LENGTH);
    if (!rc) {
      ERR_clear_error();
      // fall back to old good rand...
      log_error << "Failed to generate random key using SSL.";
      for (size_t i = 0; i < Aes_ctr::FILE_KEY_LENGTH; ++i) {
        buf[i] = rand() % 255;
      }
    }
    return std::string(buf, Aes_ctr::FILE_KEY_LENGTH);
}

static unsigned char iv[gu::Aes_ctr_decryptor::AES_BLOCK_SIZE] = {0};
std::string encrypt_key(const std::string &key_to_be_encrypted, const std::string &key)
{
    if (key_to_be_encrypted.length() != Aes_ctr::FILE_KEY_LENGTH ||
        key.length() != Aes_ctr::FILE_KEY_LENGTH) {
        log_fatal << "Encryption key length mismatch."
                    << " key_to_be_encrypted.length(): " << key_to_be_encrypted.length()
                    << " key.length(): " << key.length()
                    << " Expected: " << Aes_ctr::FILE_KEY_LENGTH
                    << " Aborting.";
        abort();
    }

    const unsigned char* key_ptr = reinterpret_cast<const unsigned char*>(key.c_str());
    const unsigned char* key_to_be_encrypted_ptr = reinterpret_cast<const unsigned char*>(key_to_be_encrypted.c_str());
    char result_buf[gu::Aes_ctr::FILE_KEY_LENGTH];
    unsigned char* result_buf_ptr = reinterpret_cast<unsigned char*>(result_buf);

    Aes_ctr_encryptor encryptor;
    encryptor.open(key_ptr, iv);
    encryptor.encrypt(result_buf_ptr, key_to_be_encrypted_ptr, key_to_be_encrypted.length());
    encryptor.close();

    return std::string(result_buf, gu::Aes_ctr::FILE_KEY_LENGTH);
}

std::string decrypt_key(const std::string &key_to_be_decrypted, const std::string &key)
{
    if (key_to_be_decrypted.length() != Aes_ctr::FILE_KEY_LENGTH ||
        key.length() != Aes_ctr::FILE_KEY_LENGTH) {
        log_fatal << "Encryption key length mismatch."
                    << " key_to_be_decrypted.length(): " << key_to_be_decrypted.length()
                    << " key.length(): " << key.length()
                    << " Expected: " << Aes_ctr::FILE_KEY_LENGTH
                    << " Aborting.";
        abort();
    }

    const unsigned char* key_ptr = reinterpret_cast<const unsigned char*>(key.c_str());
    const unsigned char* key_to_be_decrypted_ptr = reinterpret_cast<const unsigned char*>(key_to_be_decrypted.c_str());
    char result_buf[gu::Aes_ctr::FILE_KEY_LENGTH];
    unsigned char* result_buf_ptr = reinterpret_cast<unsigned char*>(result_buf);

    Aes_ctr_decryptor decryptor;
    decryptor.open(key_ptr, iv);
    decryptor.decrypt(result_buf_ptr, key_to_be_decrypted_ptr, key_to_be_decrypted.length());
    decryptor.close();
    return std::string(result_buf, gu::Aes_ctr_decryptor::FILE_KEY_LENGTH);
}

std::string create_master_key_name(const UUID& const_uuid, const UUID& uuid, int keyId) {
    static const char* MASTER_KEY_PREFIX = "GaleraKey-";
    static const char* MASTER_KEY_SEQNO_SEPARATOR = "-";
    static const char* MASTER_KEY_ID_SEPARATOR = "@";
    std::ostringstream oss;

    oss << MASTER_KEY_PREFIX << uuid << MASTER_KEY_ID_SEPARATOR << const_uuid
       << MASTER_KEY_SEQNO_SEPARATOR << std::to_string(keyId);
    return oss.str();
}


MasterKeyProvider::MasterKeyProvider(std::function<std::string(const std::string&)> get_key_cb,
  std::function<bool(const std::string&)> create_key_cb)
: key_rotation_observer([](){return true;})
, get_key_cb_(get_key_cb)
, create_key_cb_(create_key_cb) {
}

void MasterKeyProvider::register_key_rotation_request_observer(std::function<bool()> fn) {
    key_rotation_observer = fn;
}

bool MasterKeyProvider::notify_key_rotation_observer() {
    return key_rotation_observer();
}

std::string MasterKeyProvider::get_key(const std::string& keyId) {
    return get_key_cb_(keyId);
}

bool MasterKeyProvider::create_key(const std::string& keyId) {
    return create_key_cb_(keyId);
}


}  // namespace