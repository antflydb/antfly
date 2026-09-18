// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

pub const Config = struct {
    max_attempts: u32 = 0,
    max_bytes: u64 = 0,
    max_destination_attempts: u32 = 0,
    max_destinations: u32 = 0,
    max_run_ms: u32 = 30_000,

    pub fn validate(self: Config) !void {
        if (self.max_run_ms == 0 or self.max_run_ms > 60_000) return error.InvalidConfig;
        if (self.max_attempts == 0) {
            if (self.max_bytes != 0 or self.max_destination_attempts != 0 or self.max_destinations != 0) return error.InvalidConfig;
        } else if (self.max_attempts > 4096 or self.max_destination_attempts == 0 or self.max_destination_attempts > self.max_attempts or
            self.max_destinations == 0 or self.max_destinations > 256 or self.max_bytes < 4096 or self.max_bytes > 64 * 1024 * 1024)
            return error.InvalidConfig;
    }
};
