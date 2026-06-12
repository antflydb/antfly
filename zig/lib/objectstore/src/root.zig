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

pub const types = @import("types.zig");
pub const client = @import("client.zig");
pub const filesystem = @import("filesystem.zig");
pub const memory = @import("memory.zig");
pub const s3 = @import("s3.zig");
pub const gcs = @import("gcs.zig");
pub const s3_compat = @import("s3_compat.zig");

pub const ObjectMetadata = types.ObjectMetadata;
pub const PutOptions = types.PutOptions;
pub const GetOptions = types.GetOptions;
pub const DeleteOptions = types.DeleteOptions;
pub const ListOptions = types.ListOptions;
pub const ByteRange = types.ByteRange;
pub const PutResult = types.PutResult;
pub const GetResult = types.GetResult;
pub const ObjectPart = types.ObjectPart;
pub const ObjectAttributes = types.ObjectAttributes;
pub const ListEntry = types.ListEntry;
pub const ListResult = types.ListResult;

pub const Client = client.Client;
pub const FilesystemClient = filesystem.FilesystemClient;
pub const MemoryClient = memory.MemoryClient;
pub const S3 = s3;
pub const Gcs = gcs;

pub const S3Scheme = s3_compat.Scheme;
pub const S3AddressingStyle = s3_compat.AddressingStyle;
pub const S3Credentials = s3_compat.Credentials;
pub const S3Config = s3_compat.Config;
pub const S3EndpointResolution = s3_compat.EndpointResolution;
pub const S3RequestShape = s3_compat.RequestShape;
pub const S3Path = s3_compat.S3Path;
pub const resolveS3EndpointAlloc = s3_compat.resolveEndpointAlloc;
pub const s3CredentialsFromEnvAlloc = s3_compat.credentialsFromEnvAlloc;
pub const parseCanonicalS3UrlAlloc = s3_compat.parseCanonicalS3UrlAlloc;
pub const extractBucketFromS3UrlAlloc = s3_compat.extractBucketFromUrlAlloc;
pub const s3ObjectUriAlloc = s3_compat.objectUriAlloc;
pub const s3PutObjectShapeAlloc = s3_compat.putObjectShapeAlloc;

test "objectstore module compiles" {
    _ = types;
    _ = client;
    _ = filesystem;
    _ = memory;
    _ = s3;
    _ = gcs;
    _ = s3_compat;
    _ = ObjectMetadata;
    _ = PutOptions;
    _ = GetOptions;
    _ = DeleteOptions;
    _ = ListOptions;
    _ = ByteRange;
    _ = PutResult;
    _ = GetResult;
    _ = ObjectPart;
    _ = ObjectAttributes;
    _ = ListEntry;
    _ = ListResult;
    _ = Client;
    _ = FilesystemClient;
    _ = MemoryClient;
    _ = S3;
    _ = Gcs;
    _ = S3Scheme;
    _ = S3AddressingStyle;
    _ = S3Credentials;
    _ = S3Config;
    _ = S3EndpointResolution;
    _ = S3RequestShape;
    _ = S3Path;
    _ = resolveS3EndpointAlloc;
    _ = s3CredentialsFromEnvAlloc;
    _ = parseCanonicalS3UrlAlloc;
    _ = extractBucketFromS3UrlAlloc;
    _ = s3ObjectUriAlloc;
    _ = s3PutObjectShapeAlloc;
}
