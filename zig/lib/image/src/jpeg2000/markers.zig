// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub const soc: u16 = 0xff4f;
pub const sot: u16 = 0xff90;
pub const siz: u16 = 0xff51;
pub const cod: u16 = 0xff52;
pub const coc: u16 = 0xff53;
pub const qcd: u16 = 0xff5c;
pub const qcc: u16 = 0xff5d;
pub const tlm: u16 = 0xff55;
pub const plm: u16 = 0xff57;
pub const plt: u16 = 0xff58;
pub const rgn: u16 = 0xff5e;
pub const poc: u16 = 0xff5f;
pub const ppm: u16 = 0xff60;
pub const ppt: u16 = 0xff61;
pub const crg: u16 = 0xff63;
pub const com: u16 = 0xff64;
pub const mct: u16 = 0xff74;
pub const mcc: u16 = 0xff75;
pub const mco: u16 = 0xff77;
pub const sop: u16 = 0xff91;
pub const eph: u16 = 0xff92;
pub const sod: u16 = 0xff93;
pub const eoc: u16 = 0xffd9;

pub fn isStandalone(marker: u16) bool {
    return marker == soc or marker == sod or marker == eoc or (marker >= 0xff30 and marker <= 0xff3f);
}
