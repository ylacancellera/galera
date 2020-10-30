/* Copyright (c) 2020 Percona LLC and/or its affiliates. All rights reserved.

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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*/
#ifndef GARB_RAII_INCLUDED
#define GARB_RAII_INCLUDED 1
#include "garb_gcs.hpp"

/*
  RAII class for freeing gcs action buffers.
*/
class Garb_gcs_action_buffer_guard {
 public:
  explicit Garb_gcs_action_buffer_guard(gcs_action *act) : m_act(act) {}

  Garb_gcs_action_buffer_guard(const Garb_gcs_action_buffer_guard &) =
      delete;  // copy constructor
  Garb_gcs_action_buffer_guard operator=(Garb_gcs_action_buffer_guard &&) =
      delete;  // move assignment
  Garb_gcs_action_buffer_guard operator=(const Garb_gcs_action_buffer_guard &) =
      delete;  // copy assignment

  ~Garb_gcs_action_buffer_guard() {
    if (m_act && m_act->buf) {
      free(const_cast<void *>(m_act->buf));
      m_act->buf = nullptr;
    }
  }

 private:
  gcs_action *m_act;
};
#endif /* GARB_RAII_INCLUDED */
