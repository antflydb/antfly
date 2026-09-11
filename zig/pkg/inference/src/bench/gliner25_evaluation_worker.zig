// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded held-out execution. This worker never receives gold annotations.
//! The Python driver audits the prepared lock, corpus and exact denominator.
const std = @import("std");
const inference = @import("inference_internal");
const factory = inference.architectures.session_factory;
const bundle = inference.models.gliner_boundary_bundle;
const processor = inference.pipelines.gliner_boundary_processor;
const pipeline = inference.pipelines.gliner_boundary_pipeline;
const engine = inference.architectures.gliner_boundary_engine;
const c_file = inference.util.c_file;
const Allocator = std.mem.Allocator;
const source_commit = "3c913c7369301133d3b7699252074c4303ada50e";
const scope = "gliner25_blinded_execution/v1";
const massive_scope = "gliner25_massive11_blinded_execution/v2";
const BoundedAllocator = inference.runtime.bounded_allocator.BoundedAllocator;

const Case = struct {
    id: []const u8,
    request_sha256: []const u8,
    text: []const u8,
    schema: std.json.Value,
    options: struct { threshold: f32, overlap: []const u8, best_effort: bool },
    offset_unit: []const u8,
};
const Policy = struct { max_words: usize, max_encoded_tokens: usize, max_text_bytes: usize, max_queries: usize, timeout_ms: usize };
const Fixture = struct {
    format_version: u32,
    scope: []const u8,
    qualification: bool,
    source_commit: []const u8,
    model: []const u8,
    source_files: []const bundle.FilePin,
    lock_sha256: []const u8,
    prepared_sha256: []const u8,
    requests_sha256: []const u8,
    adapter_sha256: []const u8,
    harness_sha256: []const u8,
    policy: Policy,
    cases: []const Case,
};

// The v2 envelope is additive: v1 retains its exact parser, schema, limits and
// emitted records. MASSIVE shares its large fixed schema once per shard.
const MassiveCase = struct { id: []const u8, source_id: []const u8, request_sha256: []const u8, text: []const u8 };
// The locked preparation serializes request_options from its sorted JSON
// manifest. This order is part of encoded(request)'s immutable digest.
const MassiveOptions = struct { best_effort: bool, overlap: []const u8, threshold: f32, word_splitter: processor.WordSplitter };
const MassiveLimits = struct {
    max_classification_labels: usize,
    exact_node_budget: usize,
    beam_node_budget: usize,
    beam_width: usize,
    max_candidates_per_task: usize,
    max_local_assignments: usize,
    max_subset_visits: usize,
    max_output_values: usize,
    max_output_string_bytes: usize,
    max_request_host_bytes: usize,
    max_event_bytes: usize,
    max_shard_bytes: usize,
};
const MassiveTransport = struct {
    global_records: usize,
    transport_sha256: []const u8,
    index: usize,
    start: usize,
    end: usize,
    shard_sha256: []const u8,
    request_ids_sha256: []const u8,
    request_sha256s_sha256: []const u8,
};
const MassiveFixture = struct {
    format_version: u32,
    scope: []const u8,
    qualification: bool,
    source_commit: []const u8,
    model: []const u8,
    source_files: []const bundle.FilePin,
    registry_sha256: []const u8,
    profile: []const u8,
    lock_sha256: []const u8,
    prepared_sha256: []const u8,
    requests_sha256: []const u8,
    adapter_sha256: []const u8,
    harness_sha256: []const u8,
    policy: Policy,
    limits: MassiveLimits,
    schema: std.json.Value,
    options: MassiveOptions,
    offset_unit: []const u8,
    transport: MassiveTransport,
    cases: []const MassiveCase,
};

const MassiveShardPin = struct { shard: []const u8, ids: []const u8, requests: []const u8 };
const MassiveProfilePin = struct {
    name: []const u8,
    locale: []const u8,
    splitter: processor.WordSplitter,
    lock: []const u8,
    prepared: []const u8,
    requests: []const u8,
    schema: []const u8,
    transport: []const u8,
    shards: [3]MassiveShardPin,
};

// BEGIN MASSIVE EXECUTION REGISTRY PINS
const massive_registry_sha256 = "0ea74f438d905a874550b2c9dc5c6b676f74d941b5a3e13cd3b5563627fedded";
const massive_adapter_sha256 = "4543ec9710ce2c973f6e4a21f8169cc966b21896b20882b831796ca31fa8d606";
const massive_harness_sha256 = "8d6b137c55f64d7c13752c306246266a56d98796d349e9c301069bdafadb6927";
const massive_model_pins = [_]struct { name: []const u8, files_sha256: []const u8 }{
    .{ .name = "base", .files_sha256 = "afd347c858c923a2c6eef535c0e3e6a5d3ccdc68021a32ec414303f0c38783e1" },
    .{ .name = "multi", .files_sha256 = "0463e20c6ea2b5e0878f2c544138dbfd47963622f054370da4203196a6c98510" },
    .{ .name = "small", .files_sha256 = "05f2bd8df4ea1e1dc1a4dba11f8c652c9ef34fdb8a18a9c919c0c485cfdb2b64" },
};
const massive_profile_pins = [_]MassiveProfilePin{
    .{
        .name = "entities_en-US",
        .locale = "en-US",
        .splitter = .whitespace,
        .lock = "5c01d8b2dd09b0d316bbdf21e7fd913e48d2a3f66904201b093e7723a30c4336",
        .prepared = "592b8a3b08db09db3f7b0e76274e314b0402927b4b7459eda105b7aa59f63785",
        .requests = "637482230f4cd042d72d8f3babe55c70a5a7b40a3dfb775454b00aac7561f77c",
        .schema = "6676ac23c27fa3a39f0a0ad84a1f6fb2f996f9f9dc3ad6b732a91ebf2f958d85",
        .transport = "c4c45f3f193828a945002aefa4f4762ad7eb69352e58d7ffe6b3ff80d495d8d9",
        .shards = .{
            .{ .shard = "a1c3cb93a51505d2b2cf3dbf96002c7bd986233c0a33e4bf9a75f8defbcf74dd", .ids = "1d6ed74962f43c87db76ba82092d4dba95002eb2294dc1630147d73678934502", .requests = "c4e0decd791c7f454e64f9147625982dfe9cd47096480f2857b1303364af19c4" },
            .{ .shard = "0f774550fbcae0e314489bc2529848c1da780fa03e4242771ce48c0668865730", .ids = "f79b5049df24216e376f67b51a53da7467a1742fe8ba78745fc71f905e5d9f20", .requests = "7b77d4ac21171e66999f367dfdbda21b9dfe3e3761a667be93002e19c9b3d9a9" },
            .{ .shard = "5f0dc3e801fccb218623b3d69a0236a6526f0d2a9e0d74bcc85695dd9937df87", .ids = "9a5a1985b153459917f1a112ce01365e2113c1648663833f5c2ecb1304caa1b3", .requests = "b168323d5b2993f99641fb032e1941a78a424266ec0ac1aadb4c4832788e64dd" },
        },
    },
    .{
        .name = "entities_de-DE",
        .locale = "de-DE",
        .splitter = .whitespace,
        .lock = "a388aee59910d9894e6b76c212bd15b12cc22569919aeb91a2c9c06b4e9982d2",
        .prepared = "0e6243a9961773363c959d3efe7a1119be30dbe594be7c1c28d645cbdfd6b91b",
        .requests = "9a0673065844f3b6d9f2eeddd120f9f39becb6bd6e00ac9ec4456e1fe089c0db",
        .schema = "6676ac23c27fa3a39f0a0ad84a1f6fb2f996f9f9dc3ad6b732a91ebf2f958d85",
        .transport = "838a1683f3cef07e7a14787891d96b8f01163454aad102d84ee2d59fe9682ced",
        .shards = .{
            .{ .shard = "1a2d44b35bb3d4d490f0988bf5b0b6455082d06b201b378c48c236398652d13c", .ids = "e9629c73eaa27493478412c44c82303a6e043e8f536b1091228cd6770f65eef2", .requests = "df422f80e03e88bdb119ff41f4e9c605b906d4308830693776a860071ecf8b72" },
            .{ .shard = "da229c4f962d408209b1a4f72ff1cde874803870160d5a08e327ef1b38668904", .ids = "4cfa104bca9b28db639f715665235094f82a52339213b566b5ea4b17667fb973", .requests = "eda132059426bf02ab02fa150194e5045bf93e68c07f70e109d6f9993416c26b" },
            .{ .shard = "c734c0509e13a3fb3f9dcd5d5bce3b68efe8751a6106c8082db012432d55b834", .ids = "6048c1327ce5925b858298f567c40848387996433f0ff0a2eee179c02f943893", .requests = "897d8dcd2a39f70cbfbf7c5689d917c6d5a5544a4446ce636100912ec2bf72a3" },
        },
    },
    .{
        .name = "entities_ar-SA",
        .locale = "ar-SA",
        .splitter = .whitespace,
        .lock = "e6a1e7fd6e4f54495910b15a51a577779d0feb82b1caa0b3822614e2e45b01a1",
        .prepared = "ef7a49199f877e797f8670dbac24fe483102aa6dc260dfed54d0751caed46596",
        .requests = "e8582288fe62ed72c62bca3ba5b904e0fbe5f1a6dac2f5ece8057aa89a0869c6",
        .schema = "6676ac23c27fa3a39f0a0ad84a1f6fb2f996f9f9dc3ad6b732a91ebf2f958d85",
        .transport = "4ae92011582979aec2673021fbe9b8b50ce006453bf8c4c8dcd951b5b14e563d",
        .shards = .{
            .{ .shard = "6677237e93ff6ec1fc9b9a92638eb9daf1a1596fc54c5d446d39ba4df12e9b4d", .ids = "285011a358d45af6505cae9e1cef1d63fe94b80571e81180530b6e72981b1a75", .requests = "eddba9f0dcae1691f9326ce4cafb1db481d6126effcd180411f1e75e8241343f" },
            .{ .shard = "6bc5c31661aac995a199d7287d43e1176c0af213f384c23c6b61e1c888158718", .ids = "33e6386a72c81ab144aa91ded7bb635d51140105be5ab0867120be92a112d614", .requests = "181e4116eed5913b61a76e8cd1775144258b0803599c9858eb42c46c30008070" },
            .{ .shard = "e61a2ea775ebbaffffaa1a58aeeb7e9ee884ea095ac5b29d1b10aa096fd92304", .ids = "4dae32f2e2f71cf364d511a564bc0bf95654772c6fff2c4215f33045eb53d379", .requests = "f692610daf123521ecc2cc330a4e8e1e958e7e80ebce6e742db50cfd35d633fc" },
        },
    },
    .{
        .name = "entities_hi-IN",
        .locale = "hi-IN",
        .splitter = .whitespace,
        .lock = "75973a31a5ac32e97cbe46f1fdd7c898dc341bea493cfe6eead874b69eeb6112",
        .prepared = "f734136960d164511056fa77626285f9420ef06cbfb27b5fcc11680f3063f1b6",
        .requests = "57b432f2fd3e5985b73eb2c0ad6549e5ab410c93f03dc0693e6559d1e20b9070",
        .schema = "6676ac23c27fa3a39f0a0ad84a1f6fb2f996f9f9dc3ad6b732a91ebf2f958d85",
        .transport = "772020ab345e2045afcc5741adba2cb40b1bdb153770487a8f504fc3c715bf35",
        .shards = .{
            .{ .shard = "7e0b3b2e8077219029b833ce6f277323e543a69ab172dbf2e36f1d965f4452e6", .ids = "44a708b80a3d1db7f4cd7534ea093e17d6310cd215f4887afb67dcf0a8f35de9", .requests = "661ef2db9337c0d6bb7511d9276cd8df03019c1a3004b935536a282e45e79b69" },
            .{ .shard = "6d6c6bcee58164e58109552a76f7b8e6d479e96051a3d1c4ef61bad73d92285d", .ids = "e7a9c31884146ac804329709e65389cb3f6861d871affba32599ddf61b2ddda9", .requests = "d6aae03eb3402bbaa5561f6d2c0399e1db7e665ada4c7230c22dba8b160f7e89" },
            .{ .shard = "667c3691481859880c18980e683621ad49166ce41bcbdaa974ca8af92c7e8554", .ids = "2f30dc5e9920cb35891de050bc737c1e4a912a741c443843cf79da5370ac0881", .requests = "b04665d8ae0acfa739eeca7e7c80f20b8d0f351b8d34d024530a220372682339" },
        },
    },
    .{
        .name = "entities_zh-CN",
        .locale = "zh-CN",
        .splitter = .whitespace,
        .lock = "e72d7067c6a6de6bf10b143b960722001731348e32cbbe321801ae5e1bfc9302",
        .prepared = "3bc128d5c5a7cb45005b6ebaa38a74695f3efea19aa9de961d87c74402b10c44",
        .requests = "d9186ac9bafcc8868612ae9549c1500cef5eb830118e6c70f42c6722bd46c295",
        .schema = "6676ac23c27fa3a39f0a0ad84a1f6fb2f996f9f9dc3ad6b732a91ebf2f958d85",
        .transport = "7993872502570da12f134d486c9ca77e16524344a26db321c7bd0dfbc919a25d",
        .shards = .{
            .{ .shard = "ed3d31303b50018c6c14dd5720f5dcd122676185b950f78bdba3fc914ba595c2", .ids = "ffd41d81834af8a4fd4fd138fbcf8c783b7d928ad76706db5e8a18a343c1ddf1", .requests = "23703a5fc8f9ee58ba97526ca888c592c0786367f3b06c5b0fe9671d3ed90c02" },
            .{ .shard = "2831a906f1dce8b81295807462e8f71421f268eaad0728eb0025a2416888a945", .ids = "a7ef8375ab30df6f9681e5ef40d967caccd7141c084c7f2d66cf631503d2473b", .requests = "7fb4bd232ae436a2c1752bfec55730911a66c3bda911084b326d9e389f1f6d88" },
            .{ .shard = "b0f38d913d87132d885ac628081837cd9f8e3e2f87a083c69fae86ef87e186b8", .ids = "93a347c6d967f2e03c04cb5c06a4b4c425648325377abbae227d179af69af3ea", .requests = "e0bdfd2079a5844656ad3c9eac6b49ba274ded45b7a1d1bf50e85b4c5f643e4a" },
        },
    },
    .{
        .name = "entities_ja-JP",
        .locale = "ja-JP",
        .splitter = .whitespace,
        .lock = "45f7ce097e3cda2c6f614357f118b6177cc0793079ed77c3ef0f6cf766146184",
        .prepared = "d8b725b40540dfc159b5756c868211fa3798a44661938b1508d24d51299569a8",
        .requests = "65c7c8f6694d153a75710c929798aa03b325f97da3d457b9787fe900d7dcea35",
        .schema = "6676ac23c27fa3a39f0a0ad84a1f6fb2f996f9f9dc3ad6b732a91ebf2f958d85",
        .transport = "9a222b092f14a0863c54c1256f8dcddccb3939a2ae4b70f614407a40dc2a3c6a",
        .shards = .{
            .{ .shard = "77a89afdb693f3db974038274241d1728ec15dbdcad77274bdf817e903fba9d4", .ids = "c5e9884df0c9a85d744851beff98f070a15d0bbbf203600f369f95e9dac4aefd", .requests = "1dc1bb76b3d07dad2b8f714faf7839c7c3f39955bd9bf6f37bc5cf5bc669c761" },
            .{ .shard = "d1312eca7ceb3fd245e8267d555f4f16dc299d70d8bc7d21680cab6f1d4e27ba", .ids = "8ef3a3f1339c9f7ee9dc840f538ef52d33cafbafce6735002aa240bc0a87455a", .requests = "31d33a3372bc9729a3db1c6087a37ac37188f2af03d5254f9ed028c0b66ef2a1" },
            .{ .shard = "3418ebd94a8dd590135b54e8521fffc11888929567deff0c3c8d20f0d83fac17", .ids = "f77ac8c05e7d27f6e1737a14bd719a9c6f67c3298efd8714a4a138979eefe6a4", .requests = "fbc27bf5384a348eda1afea72f6230854d54b84e6100227c5525db03e35bc84d" },
        },
    },
    .{
        .name = "entities_zh-CN_char",
        .locale = "zh-CN",
        .splitter = .char,
        .lock = "981a396add71129f67b9f7585f7b52c04f4a77e0e3461e410de447c325294c65",
        .prepared = "8793c7df979c1d157360a180ec148afd54e254fc8efc238399c9b7251d738a5f",
        .requests = "a0d126fa31a191c7342955baefcab8e440e49f92d6f4d68ecb8670188cb7a879",
        .schema = "6676ac23c27fa3a39f0a0ad84a1f6fb2f996f9f9dc3ad6b732a91ebf2f958d85",
        .transport = "5be40a7e39f56d9b96025456120c8302058122768c7045d185d8e81d827d0091",
        .shards = .{
            .{ .shard = "3c8820609241d51f55cbf09e9d42c71e119f4ae9397490e8c71e1d8025709f43", .ids = "ad8be65559e61471dc114b3219743c9a541b3c6ca21e4756164908a49c60a0fe", .requests = "f36b50bd158c00e2120b224cdfc829f56eb8b890a7c6847b222aa788671c4177" },
            .{ .shard = "3025baeb9dcafee2e07a7b170c75fa4f0a2df6a9e09ddad66b82a9f116a3cb68", .ids = "2260495a299376177b725b7963750e439b00ea2cf751196fdf0bb3bdbc2a9271", .requests = "088e3721d3ff8c28436752b222fc72f22fdd1497a99c96d01d96b60a6cd42482" },
            .{ .shard = "a008ba314b91bc14fb584799f35f19bb36413f36b39fdacaedae513768fbbb39", .ids = "635a3ec1e2a896bdfa92652dbfa03edec05862223a5faca15b4446c2fe3ea9a7", .requests = "0d4f5e388af6e3d8e0d003b93de4b897e78ccf9f459f23f2b1db1ae9a5f976c7" },
        },
    },
    .{
        .name = "entities_ja-JP_char",
        .locale = "ja-JP",
        .splitter = .char,
        .lock = "8ad2f9aac89c5c56191ac0f8bd1c05618f4c8a5891a6850baf6c909901ac2205",
        .prepared = "e7870af05c57a5d692f468cbb67995162649b317faca2528d7fbbaba97c9033d",
        .requests = "6832761a1e76de89274d014447e480e2cd21a4f58f2c40a98faecbf20d6254e0",
        .schema = "6676ac23c27fa3a39f0a0ad84a1f6fb2f996f9f9dc3ad6b732a91ebf2f958d85",
        .transport = "265088a6df141b84622ec2ccf1622310c322cdfa1e1ec05909d59cef0c86aadc",
        .shards = .{
            .{ .shard = "f4ccced39bc35c572891b42b71cf4227d9ebce6270c55fe393e6b416552404bf", .ids = "17f9cd005a0843141dbc677c8ee6c18772d6a2dda8d65c3082f4b74c0c550224", .requests = "92f5e4b9802d9549ae21fbdab31d8161ecd73145c4e054c0807fa880e139acde" },
            .{ .shard = "11956313774cc31a12977a1e7feb07a7029ef2c701ee6dfd7313ad5ff10196a3", .ids = "be235726efb504e62114abd9d97c925701416debc013b98cdb04aa603992119f", .requests = "32fcce05b63cb28731aa769ebfc2f8123fc96e2452a48d56f7aae907cce7e8f9" },
            .{ .shard = "2b05dc38080e0b3801b50d8b46b6d911b344d1f7ccf933c6848a876b6b0801a2", .ids = "930d1027b0d7a1859aea49b5812883b5f45e41549fd86e502ed846268c29a8c3", .requests = "db0f9fbb7221f529c6a871ee3e014c9622c971df0f39e208b84bb799a8007042" },
        },
    },
    .{
        .name = "intent_en-US",
        .locale = "en-US",
        .splitter = .whitespace,
        .lock = "87c10ad37fd375b958034c9a52d271ad1e6add59c374471406a5965b8d8baca5",
        .prepared = "35ea3b5b207c3bbcd98ce0d81d4f5d47bba68e93f12d8c34156438b50557334c",
        .requests = "61425a6b5a1c8c52d1e4fd016e104fdcf0051fc31248b18ad5246f18ea295d34",
        .schema = "8c1b2b0dfbd1cf1543f4dad784a216cc902ec5acc94a432b528e1b6dd3e00fc9",
        .transport = "b6282e86e96ec6259d27a5351c71ab4559b5ccae0b6d0f08e778e7114b2e96bc",
        .shards = .{
            .{ .shard = "ce9b5db9b13fb25a9c64eac9687b8cb0bd1d4db0c480b24e189ae3eacd37390e", .ids = "f6e0a57dc04577ea7b5d47f80b55369aa1bfeaca4d0682bb5c9dfb543f83f15c", .requests = "859c68bb3c5132e956c49393c11914a1dae2c25258de19d92112db48e2a7408f" },
            .{ .shard = "9d3e50e1b8939e47dec3923c9ed848446d48f10d07dce8975b3c114fc98c3682", .ids = "955a2bb823204a38e5e48270df3ead5792cf39e88c96772338dd2788f5887f02", .requests = "3b83a2d7d9d36bce22edae718875f0dc2b23b2fb9b15cd4640887360edfbfd6f" },
            .{ .shard = "298fcd0c1aef91ca129297435281bfa51f9036494dcb90679b8025c6873949a2", .ids = "b0ae956682d32890df69d39970e7d3b4262a92dcd57008a2b2a4f321bceef021", .requests = "661c7081a3f33cfaa450f3929c732e1213527703c10bbf10c25be6553c68aae8" },
        },
    },
    .{
        .name = "intent_scenario_en-US",
        .locale = "en-US",
        .splitter = .whitespace,
        .lock = "1ea64686ab886a9f1afebcc2cd8ea00b2d936d9baff4301ac3903d3873448b63",
        .prepared = "ab8bcb66fa43ab727e591431121fd65fa7f8b923e55488083b9986622a9a8581",
        .requests = "e462de914cd9bacd5ea84994036c4e34d6c197065f9d4a844eb1796cbd606187",
        .schema = "c645438a373b4657c0fd15f7886bbe7c7269d61d857d202b9ace82d89f8fd58f",
        .transport = "70675cbf5c9f3a449ca34478bf327d898a42e25f9b1df709fda1b17386cac22c",
        .shards = .{
            .{ .shard = "7f51027171d13d69c6993e8d9cdc69e5a82cb5bbbedb7bc79cd6258ca395a0c8", .ids = "eec08af771441b701f55d1fec33dca4f7504fd1bd6f916b0e895c7e09db30a88", .requests = "35340f2334d0ce2c9d152d4f1e72b6d359927057a7f1c1fd5cd5f48ef0e2a83d" },
            .{ .shard = "aebaac45e10430046831f393941ae9ca313f919ed762c03f5dc96de6d177bc77", .ids = "36fe7264dfe31ad6458700b62aadcfccd4a1fea4e6fc1e1347623a1400436048", .requests = "aef6246adf89c497049d4b809ef901af0c1d4e25cc69e2fa571a8780cfa77387" },
            .{ .shard = "c73c4d5194b1000c3281c7459e61dae4ad374da50339a17de0b394e29ab06895", .ids = "399b66b5cd96c93a3df601846b86111c06e297f7eb8b66f5967a110e7ce18132", .requests = "b55a5c6164e4724ffb16408ccf12eec3ed83b1eaa22301b7096b80d1eeb70692" },
        },
    },
};
// END MASSIVE EXECUTION REGISTRY PINS

fn same(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

fn jsonDigest(a: Allocator, value: anytype, expected: []const u8) !void {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    const actual = bundle.Digest.of(bytes);
    if (!same(&actual.sha256, expected)) return error.InvalidEvaluationFixture;
}

fn validateMassive(a: Allocator, fixture: MassiveFixture) !void {
    if (fixture.format_version != 2 or !same(fixture.scope, massive_scope) or fixture.qualification or
        !same(fixture.source_commit, source_commit) or !same(fixture.registry_sha256, massive_registry_sha256) or
        !same(fixture.adapter_sha256, massive_adapter_sha256) or !same(fixture.harness_sha256, massive_harness_sha256) or
        fixture.policy.max_words != 128 or fixture.policy.max_encoded_tokens != 512 or fixture.policy.max_text_bytes != 1024 * 1024 or
        fixture.policy.max_queries != 64 or fixture.policy.timeout_ms != 120000 or fixture.source_files.len != 5 or
        fixture.transport.global_records != 2974 or fixture.transport.index >= 3 or fixture.schema != .object or
        fixture.options.threshold != 0.5 or fixture.options.best_effort or !same(fixture.options.overlap, "flat") or
        !same(fixture.offset_unit, "utf8_bytes")) return error.InvalidEvaluationFixture;
    const limits = fixture.limits;
    if (limits.max_classification_labels != 78 or limits.exact_node_budget != 200000 or limits.beam_node_budget != 200000 or
        limits.beam_width != 16 or limits.max_candidates_per_task != 64 or limits.max_local_assignments != 4096 or
        limits.max_subset_visits != 65536 or limits.max_output_values != 2048 or limits.max_output_string_bytes != 1024 * 1024 or
        limits.max_request_host_bytes != 512 * 1024 * 1024 or limits.max_event_bytes != 4 * 1024 * 1024 or
        limits.max_shard_bytes != 64 * 1024 * 1024) return error.InvalidEvaluationFixture;
    const model_pin = for (massive_model_pins) |pin| {
        if (same(pin.name, fixture.model)) break pin;
    } else return error.InvalidEvaluationFixture;
    // FilePin's declaration order serves existing bundle/v1 output. The
    // MASSIVE registry has its own sorted object field order; normalize it
    // explicitly rather than changing the shared FilePin representation.
    const ordered_pins = try a.alloc(struct { path: []const u8, sha256: []const u8, size_bytes: u64 }, fixture.source_files.len);
    defer a.free(ordered_pins);
    for (fixture.source_files, ordered_pins) |source, *ordered| ordered.* = .{ .path = source.path, .sha256 = source.sha256, .size_bytes = source.size_bytes };
    try jsonDigest(a, ordered_pins, model_pin.files_sha256);
    const pin = for (massive_profile_pins) |candidate| {
        if (same(candidate.name, fixture.profile)) break candidate;
    } else return error.InvalidEvaluationFixture;
    if (!same(pin.lock, fixture.lock_sha256) or !same(pin.prepared, fixture.prepared_sha256) or
        !same(pin.requests, fixture.requests_sha256) or pin.splitter != fixture.options.word_splitter or
        !same(pin.transport, fixture.transport.transport_sha256)) return error.InvalidEvaluationFixture;
    try jsonDigest(a, fixture.schema, pin.schema);
    const index = fixture.transport.index;
    const start = index * 1024;
    const end = @min(start + 1024, 2974);
    if (fixture.transport.start != start or fixture.transport.end != end or fixture.cases.len != end - start or
        !same(pin.shards[index].shard, fixture.transport.shard_sha256) or
        !same(pin.shards[index].ids, fixture.transport.request_ids_sha256) or
        !same(pin.shards[index].requests, fixture.transport.request_sha256s_sha256)) return error.InvalidEvaluationFixture;
    const ids = try a.alloc([]const u8, fixture.cases.len);
    defer a.free(ids);
    const hashes = try a.alloc([]const u8, fixture.cases.len);
    defer a.free(hashes);
    const source_prefix = try std.fmt.allocPrint(a, "massive/1.1/{s}/test/", .{pin.locale});
    defer a.free(source_prefix);
    for (fixture.cases, 0..) |case, i| {
        if (!digest(case.id) or !digest(case.request_sha256) or !std.mem.startsWith(u8, case.source_id, source_prefix))
            return error.InvalidEvaluationFixture;
        try jsonDigest(a, .{ .lock = fixture.lock_sha256, .id = case.source_id }, case.id);
        // Declaration order matches the immutable Python encoded(request)
        // contract, including original UTF-8 and the explicit splitter.
        try jsonDigest(a, .{ .text = case.text, .schema = fixture.schema, .options = fixture.options, .offset_unit = fixture.offset_unit }, case.request_sha256);
        ids[i] = case.id;
        hashes[i] = case.request_sha256;
        for (ids[0..i]) |previous| if (same(previous, case.id)) return error.InvalidEvaluationFixture;
    }
    try jsonDigest(a, ids, pin.shards[index].ids);
    try jsonDigest(a, hashes, pin.shards[index].requests);
}

fn digest(value: []const u8) bool {
    if (value.len != 64) return false;
    for (value) |c| if (!((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'))) return false;
    return true;
}

fn validate(fixture: Fixture) !void {
    if (fixture.format_version != 1 or !std.mem.eql(u8, fixture.scope, scope) or fixture.qualification or
        !std.mem.eql(u8, fixture.source_commit, source_commit) or fixture.cases.len == 0 or fixture.cases.len > 1024 or
        fixture.source_files.len != 5 or fixture.policy.max_words != 128 or fixture.policy.max_encoded_tokens != 512 or
        fixture.policy.max_text_bytes != 1024 * 1024 or fixture.policy.max_queries != 64 or fixture.policy.timeout_ms != 120000)
        return error.InvalidEvaluationFixture;
    for ([_][]const u8{ fixture.lock_sha256, fixture.prepared_sha256, fixture.requests_sha256, fixture.adapter_sha256, fixture.harness_sha256 }) |value|
        if (!digest(value)) return error.InvalidEvaluationFixture;
    for (fixture.source_files, 0..) |pin, index| {
        if (!digest(pin.sha256) or pin.size_bytes == 0 or pin.size_bytes > try bundle.fileLimit(pin.path)) return error.InvalidEvaluationFixture;
        for (fixture.source_files[0..index]) |previous| if (std.mem.eql(u8, previous.path, pin.path)) return error.InvalidEvaluationFixture;
    }
    _ = try bundle.pinFor(fixture.source_files, "model.safetensors");
    for (bundle.sidecar_names) |name| _ = try bundle.pinFor(fixture.source_files, name);
    const entity_types = [_][]const u8{ "field", "task", "product", "algorithm", "researcher", "metrics", "programlang", "conference", "university", "country", "person", "organisation", "location", "misc" };
    for (fixture.cases, 0..) |case, index| {
        if (!digest(case.id) or !digest(case.request_sha256) or case.options.threshold != 0.5 or case.options.best_effort or
            !std.mem.eql(u8, case.options.overlap, "flat") or !std.mem.eql(u8, case.offset_unit, "utf8_bytes") or case.schema != .object or case.schema.object.count() != 1)
            return error.InvalidEvaluationFixture;
        const entities = case.schema.object.get("entities") orelse return error.InvalidEvaluationFixture;
        if (entities != .array or entities.array.items.len != entity_types.len) return error.InvalidEvaluationFixture;
        for (entities.array.items, entity_types) |value, name|
            if (value != .string or !std.mem.eql(u8, value.string, name)) return error.InvalidEvaluationFixture;
        for (fixture.cases[0..index]) |previous| if (std.mem.eql(u8, previous.id, case.id)) return error.InvalidEvaluationFixture;
    }
}

fn emit(a: Allocator, io: std.Io, value: anytype) !void {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    if (bytes.len > 8 * 1024 * 1024) return error.ExtractionOutputLimitExceeded;
    try std.Io.File.stdout().writeStreamingAll(io, bytes);
    try std.Io.File.stdout().writeStreamingAll(io, "\n");
}

fn verifyFiles(a: Allocator, path: []const u8, pins: []const bundle.FilePin) !void {
    for (pins) |pin| {
        if (pin.size_bytes > try bundle.fileLimit(pin.path)) return error.InvalidEvaluationFixture;
        const full = try std.fs.path.join(a, &.{ path, pin.path });
        defer a.free(full);
        // Read-only mappings avoid materializing a second checkpoint copy.
        var mapping = try c_file.MmapRegion.init(a, full);
        defer mapping.deinit();
        try bundle.Digest.of(mapping.data).verify(pin);
    }
}

fn runCase(a: Allocator, io: std.Io, session: anytype, config: anytype, tokenizer: anytype, fixture: Fixture, case: Case, metal: bool, precision: anytype, watchdog: ?*inference.HardCancellationWatchdog, massive: ?MassiveFixture) !void {
    const control = inference.InferenceExecutionControl{ .hard_cancellation = if (watchdog) |owner| owner.boundary() else null, .deadline_ns = inference.platform.time.monotonicNs() + 120 * std.time.ns_per_s };
    const schema_json = try std.json.Stringify.valueAlloc(a, case.schema, .{});
    defer a.free(schema_json);
    var schema = try inference.pipelines.extraction_schema.compile(a, schema_json, .{});
    defer schema.deinit();
    var prepared = try processor.prepare(a, tokenizer, &.{.{ .text = case.text, .schema = &schema }}, .{
        .max_batch_items = 1,
        .max_text_bytes = fixture.policy.max_text_bytes,
        .max_text_words = fixture.policy.max_words,
        .max_total_words = fixture.policy.max_words,
        .max_sequence_tokens = fixture.policy.max_encoded_tokens,
        .max_batch_tokens = fixture.policy.max_encoded_tokens,
        .max_queries = fixture.policy.max_queries,
        .word_splitter = if (massive) |value| value.options.word_splitter else .whitespace,
        .max_classification_labels = if (massive) |value| value.limits.max_classification_labels else 512,
        .control = control,
    });
    defer prepared.deinit();
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    var options = pipeline.Options{ .threshold = 0.5, .overlap = .flat, .best_effort = false, .offset_unit = .utf8_bytes, .control = control };
    if (massive) |value| {
        options.max_output_values = value.limits.max_output_values;
        options.max_output_string_bytes = value.limits.max_output_string_bytes;
        options.classification_solver = .{ .exact_node_budget = value.limits.exact_node_budget, .beam_node_budget = value.limits.beam_node_budget, .beam_width = value.limits.beam_width, .max_local_assignments = value.limits.max_local_assignments, .max_subset_visits = value.limits.max_subset_visits };
    }
    var result = if (metal) blk: {
        const device = try inference.architectures.gliner_boundary_request_device.run(&managed.backend, a, config, &prepared, &.{&schema}, .{ .precision = precision, .pipeline = options });
        break :blk device.outputs;
    } else blk: {
        var encoded = try engine.encodeNative(&managed.backend, a, config, &prepared, .{ .control = control });
        defer encoded.deinit();
        break :blk try pipeline.runNative(&managed.backend, a, config, &prepared, &.{&schema}, .{
            .text_states = encoded.text_states,
            .query_states = encoded.query_states,
            .classification_states = encoded.classification_states,
            .text_lengths = encoded.text_lengths,
        }, options);
    };
    defer result.deinit();
    if (result.samples.len != 1) return error.InvalidExtractionOutput;
    const event = .{ .event = "result", .case_id = case.id, .request_sha256 = case.request_sha256, .input_ids = prepared.input_ids, .output = result.samples[0] };
    if (massive) |value| {
        const event_bytes = try std.json.Stringify.valueAlloc(a, event, .{});
        defer a.free(event_bytes);
        if (event_bytes.len + 1 > value.limits.max_event_bytes) return error.ExtractionOutputLimitExceeded;
        try std.Io.File.stdout().writeStreamingAll(io, event_bytes);
        try std.Io.File.stdout().writeStreamingAll(io, "\n");
    } else try emit(a, io, event);
}

pub fn run(a: Allocator, io: std.Io, directory: []const u8, fixture_path: []const u8, metal: bool) !void {
    const bytes = try c_file.readFileMax(a, fixture_path, 8 * 1024 * 1024);
    defer a.free(bytes);
    var header = try std.json.parseFromSlice(struct { format_version: u32 }, a, bytes, .{ .ignore_unknown_fields = true, .duplicate_field_behavior = .@"error" });
    defer header.deinit();
    if (header.value.format_version == 2) {
        var parsed = try std.json.parseFromSlice(MassiveFixture, a, bytes, .{ .duplicate_field_behavior = .@"error" });
        defer parsed.deinit();
        const massive = parsed.value;
        try validateMassive(a, massive);
        const cases = try a.alloc(Case, massive.cases.len);
        defer a.free(cases);
        for (massive.cases, cases) |input, *case| case.* = .{ .id = input.id, .request_sha256 = input.request_sha256, .text = input.text, .schema = massive.schema, .options = .{ .threshold = massive.options.threshold, .overlap = massive.options.overlap, .best_effort = massive.options.best_effort }, .offset_unit = massive.offset_unit };
        const fixture: Fixture = .{ .format_version = 2, .scope = massive.scope, .qualification = false, .source_commit = massive.source_commit, .model = massive.model, .source_files = massive.source_files, .lock_sha256 = massive.lock_sha256, .prepared_sha256 = massive.prepared_sha256, .requests_sha256 = massive.requests_sha256, .adapter_sha256 = massive.adapter_sha256, .harness_sha256 = massive.harness_sha256, .policy = massive.policy, .cases = cases };
        return runAdmitted(a, io, directory, fixture_path, bytes, fixture, metal, massive);
    }
    var parsed = try std.json.parseFromSlice(Fixture, a, bytes, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    const fixture = parsed.value;
    try validate(fixture);
    return runAdmitted(a, io, directory, fixture_path, bytes, fixture, metal, null);
}

fn runAdmitted(a: Allocator, io: std.Io, directory: []const u8, fixture_path: []const u8, bytes: []const u8, fixture: Fixture, metal: bool, massive: ?MassiveFixture) !void {
    var manifest = try inference.models.manifest.loadFromDir(a, directory);
    defer manifest.deinit();
    const loaded_config = manifest.gliner_boundary_config orelse return error.InvalidGlinerBoundaryConfig;
    if (!std.mem.eql(u8, fixture.model, @tagName(loaded_config.backbone))) return error.GlinerBoundaryArtifactMismatch;
    const receipt: ?bundle.Receipt = if (manifest.gliner_boundary_bundle) |owner| owner.value else null;
    const actual_pins = if (receipt) |value| value.files else fixture.source_files;
    if (receipt) |value| {
        try bundle.validate(value);
        for (fixture.source_files) |expected| {
            const actual = try bundle.pinFor(value.source_files, expected.path);
            if (actual.size_bytes != expected.size_bytes or !std.mem.eql(u8, actual.sha256, expected.sha256)) return error.GlinerBoundaryArtifactMismatch;
        }
    }
    try verifyFiles(a, directory, actual_pins);
    const session = if (metal) try factory.createMetalSession(a, directory) else try factory.createNativeSession(a, directory);
    defer session.close();
    const identity = try factory.getGlinerBoundaryIdentity(session);
    if (!std.mem.eql(u8, fixture.model, @tagName(identity.backbone))) return error.GlinerBoundaryArtifactMismatch;
    if (receipt) |value| {
        if (identity.precision != value.precision) return error.GlinerBoundaryArtifactMismatch;
        try identity.weight.verify(try bundle.pinFor(value.files, bundle.model_name));
    } else {
        if (identity.precision != .fp32) return error.GlinerBoundaryArtifactMismatch;
        try identity.weight.verify(try bundle.pinFor(fixture.source_files, "model.safetensors"));
    }
    try identity.verifySidecars(try manifest.boundarySidecarDigests());
    for (identity.sidecars, bundle.sidecar_names) |actual, name| try actual.verify(try bundle.pinFor(fixture.source_files, name));
    const config = try factory.getGlinerBoundaryConfig(session);
    const watchdog = if (metal) try inference.HardCancellationWatchdog.create(a) else null;
    defer if (watchdog) |owner| owner.destroy();
    if (watchdog) |owner| try owner.start(io);
    const tokenizer_bytes = try c_file.readFileMax(a, manifest.tokenizer_json_path orelse return error.NoTokenizerFound, 32 * 1024 * 1024);
    defer a.free(tokenizer_bytes);
    try bundle.Digest.of(tokenizer_bytes).verify(try bundle.pinFor(fixture.source_files, "tokenizer.json"));
    const tokenizer = try inference.hf_tokenizer.HfTokenizer.loadFromBytesWithOptions(a, tokenizer_bytes, .{ .strict_unigram_normalizer = true });
    defer tokenizer.tokenizer().deinitTokenizer();
    const fixture_digest = bundle.Digest.of(bytes);
    if (massive) |value| {
        try emit(a, io, .{ .event = "ready", .scope = massive_scope, .backend = if (metal) "metal" else "native", .qualification = false, .artifact_kind = if (receipt != null) "bundle" else "source_fp32", .receipt = receipt, .source_files = fixture.source_files, .model = fixture.model, .source_commit = source_commit, .fixture_sha256 = fixture_digest.sha256[0..], .lock_sha256 = fixture.lock_sha256, .profile = value.profile, .transport = value.transport, .registry_sha256 = value.registry_sha256, .word_splitter = value.options.word_splitter, .math_policy = "strict_f32_activations_v1", .weight_precision = identity.precision, .activation_precision = "f32", .accumulation_precision = "f32", .head_precision = "f32", .build_mode = @tagName(@import("builtin").mode), .zig_version = @import("builtin").zig_version_string });
    } else try emit(a, io, .{ .event = "ready", .scope = scope, .backend = if (metal) "metal" else "native", .qualification = false, .artifact_kind = if (receipt != null) "bundle" else "source_fp32", .receipt = receipt, .source_files = fixture.source_files, .model = fixture.model, .source_commit = source_commit, .fixture_sha256 = fixture_digest.sha256[0..], .lock_sha256 = fixture.lock_sha256, .math_policy = "strict_f32_activations_v1", .weight_precision = identity.precision, .activation_precision = "f32", .accumulation_precision = "f32", .head_precision = "f32", .build_mode = @tagName(@import("builtin").mode), .zig_version = @import("builtin").zig_version_string });
    var failures: usize = 0;
    for (fixture.cases) |case| {
        var request_budget = BoundedAllocator{ .backing = a, .limit = if (massive) |value| value.limits.max_request_host_bytes else 0 };
        const request_allocator = if (massive != null) request_budget.allocator() else a;
        runCase(request_allocator, io, session, &config, tokenizer.tokenizer(), fixture, case, metal, identity.precision, watchdog, massive) catch |err| {
            failures += 1;
            try emit(a, io, .{ .event = "error", .case_id = case.id, .request_sha256 = case.request_sha256, .error_code = @errorName(err), .input_ids = @as(?[]const u32, null) });
        };
    }
    try verifyFiles(a, directory, actual_pins);
    const final_bytes = try c_file.readFileMax(a, fixture_path, 8 * 1024 * 1024);
    defer a.free(final_bytes);
    const final_digest = bundle.Digest.of(final_bytes);
    try fixture_digest.verify(.{ .path = "fixture", .size_bytes = final_bytes.len, .sha256 = &final_digest.sha256 });
    try emit(a, io, .{ .event = "complete", .cases = fixture.cases.len, .errors = failures, .qualification = false });
}
