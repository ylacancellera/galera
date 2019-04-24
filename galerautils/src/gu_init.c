/*
 * Copyright (C) 2013-2016 Codership Oy <info@codership.com>
 *
 * $Id$
 */

#include "gu_conf.h"
#include "gu_limits.h"
#include "gu_abort.h"
#include "gu_crc32c.h"
#ifdef PXC
#include "gu_init.h"
#endif /* PXC */

#ifdef PXC
void
gu_init (gu_log_cb_t log_cb, gu_pfs_instr_cb_t pfs_instr_cb)
#else
void
gu_init (gu_log_cb_t log_cb)
#endif /* PXC */
{
    gu_conf_set_log_callback (log_cb);
#ifdef PXC
    gu_conf_set_pfs_instr_callback (pfs_instr_cb);
#endif /* PXC */

    /* this is needed in gu::MMap::sync() */
    size_t const page_size = GU_PAGE_SIZE;
    if (page_size & (page_size - 1))
    {
        gu_fatal("GU_PAGE_SIZE(%z) is not a power of 2", GU_PAGE_SIZE);
        gu_abort();
    }

    gu_crc32c_configure();
}
