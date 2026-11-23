/* Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
 * Independent PG oracle provider for the exact native antfly relabel policy.
 * This is not selinux/sepgsql and does not supply row-access enforcement. */
#include "postgres.h"
#include "commands/seclabel.h"
#include "fmgr.h"
#include "miscadmin.h"
PG_MODULE_MAGIC;
static void check_label(const ObjectAddress *object, const char *label)
{
    char *old = GetSecurityLabel(object, "antfly");
    if ((old && strcmp(old, "restricted") == 0) ||
        (label && strcmp(label, "restricted") == 0))
        if (!superuser())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                            errmsg("antfly restricted relabeling requires administrator")));
    if (label && strcmp(label, "public") != 0 && strcmp(label, "internal") != 0 && strcmp(label, "restricted") != 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid antfly security label")));
}
PGDLLEXPORT void _PG_init(void);
void _PG_init(void) { register_label_provider("antfly", check_label); }
