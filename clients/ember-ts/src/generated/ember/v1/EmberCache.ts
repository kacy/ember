// Original file: ../../proto/ember/v1/ember.proto

import type { MethodDefinition } from '@grpc/proto-loader'
import type { AppendRequest as _ember_v1_AppendRequest, AppendRequest__Output as _ember_v1_AppendRequest__Output } from '../../ember/v1/AppendRequest';
import type { ArrayResponse as _ember_v1_ArrayResponse, ArrayResponse__Output as _ember_v1_ArrayResponse__Output } from '../../ember/v1/ArrayResponse';
import type { BgRewriteAofRequest as _ember_v1_BgRewriteAofRequest, BgRewriteAofRequest__Output as _ember_v1_BgRewriteAofRequest__Output } from '../../ember/v1/BgRewriteAofRequest';
import type { BgSaveRequest as _ember_v1_BgSaveRequest, BgSaveRequest__Output as _ember_v1_BgSaveRequest__Output } from '../../ember/v1/BgSaveRequest';
import type { BoolArrayResponse as _ember_v1_BoolArrayResponse, BoolArrayResponse__Output as _ember_v1_BoolArrayResponse__Output } from '../../ember/v1/BoolArrayResponse';
import type { BoolResponse as _ember_v1_BoolResponse, BoolResponse__Output as _ember_v1_BoolResponse__Output } from '../../ember/v1/BoolResponse';
import type { CopyRequest as _ember_v1_CopyRequest, CopyRequest__Output as _ember_v1_CopyRequest__Output } from '../../ember/v1/CopyRequest';
import type { DbSizeRequest as _ember_v1_DbSizeRequest, DbSizeRequest__Output as _ember_v1_DbSizeRequest__Output } from '../../ember/v1/DbSizeRequest';
import type { DecrByRequest as _ember_v1_DecrByRequest, DecrByRequest__Output as _ember_v1_DecrByRequest__Output } from '../../ember/v1/DecrByRequest';
import type { DecrRequest as _ember_v1_DecrRequest, DecrRequest__Output as _ember_v1_DecrRequest__Output } from '../../ember/v1/DecrRequest';
import type { DelRequest as _ember_v1_DelRequest, DelRequest__Output as _ember_v1_DelRequest__Output } from '../../ember/v1/DelRequest';
import type { DelResponse as _ember_v1_DelResponse, DelResponse__Output as _ember_v1_DelResponse__Output } from '../../ember/v1/DelResponse';
import type { EchoRequest as _ember_v1_EchoRequest, EchoRequest__Output as _ember_v1_EchoRequest__Output } from '../../ember/v1/EchoRequest';
import type { EchoResponse as _ember_v1_EchoResponse, EchoResponse__Output as _ember_v1_EchoResponse__Output } from '../../ember/v1/EchoResponse';
import type { ExistsRequest as _ember_v1_ExistsRequest, ExistsRequest__Output as _ember_v1_ExistsRequest__Output } from '../../ember/v1/ExistsRequest';
import type { ExpireRequest as _ember_v1_ExpireRequest, ExpireRequest__Output as _ember_v1_ExpireRequest__Output } from '../../ember/v1/ExpireRequest';
import type { FloatResponse as _ember_v1_FloatResponse, FloatResponse__Output as _ember_v1_FloatResponse__Output } from '../../ember/v1/FloatResponse';
import type { FlushDbRequest as _ember_v1_FlushDbRequest, FlushDbRequest__Output as _ember_v1_FlushDbRequest__Output } from '../../ember/v1/FlushDbRequest';
import type { GetDelRequest as _ember_v1_GetDelRequest, GetDelRequest__Output as _ember_v1_GetDelRequest__Output } from '../../ember/v1/GetDelRequest';
import type { GetExRequest as _ember_v1_GetExRequest, GetExRequest__Output as _ember_v1_GetExRequest__Output } from '../../ember/v1/GetExRequest';
import type { GetRangeRequest as _ember_v1_GetRangeRequest, GetRangeRequest__Output as _ember_v1_GetRangeRequest__Output } from '../../ember/v1/GetRangeRequest';
import type { GetRequest as _ember_v1_GetRequest, GetRequest__Output as _ember_v1_GetRequest__Output } from '../../ember/v1/GetRequest';
import type { GetResponse as _ember_v1_GetResponse, GetResponse__Output as _ember_v1_GetResponse__Output } from '../../ember/v1/GetResponse';
import type { HDelRequest as _ember_v1_HDelRequest, HDelRequest__Output as _ember_v1_HDelRequest__Output } from '../../ember/v1/HDelRequest';
import type { HExistsRequest as _ember_v1_HExistsRequest, HExistsRequest__Output as _ember_v1_HExistsRequest__Output } from '../../ember/v1/HExistsRequest';
import type { HGetAllRequest as _ember_v1_HGetAllRequest, HGetAllRequest__Output as _ember_v1_HGetAllRequest__Output } from '../../ember/v1/HGetAllRequest';
import type { HGetRequest as _ember_v1_HGetRequest, HGetRequest__Output as _ember_v1_HGetRequest__Output } from '../../ember/v1/HGetRequest';
import type { HIncrByRequest as _ember_v1_HIncrByRequest, HIncrByRequest__Output as _ember_v1_HIncrByRequest__Output } from '../../ember/v1/HIncrByRequest';
import type { HKeysRequest as _ember_v1_HKeysRequest, HKeysRequest__Output as _ember_v1_HKeysRequest__Output } from '../../ember/v1/HKeysRequest';
import type { HLenRequest as _ember_v1_HLenRequest, HLenRequest__Output as _ember_v1_HLenRequest__Output } from '../../ember/v1/HLenRequest';
import type { HMGetRequest as _ember_v1_HMGetRequest, HMGetRequest__Output as _ember_v1_HMGetRequest__Output } from '../../ember/v1/HMGetRequest';
import type { HScanRequest as _ember_v1_HScanRequest, HScanRequest__Output as _ember_v1_HScanRequest__Output } from '../../ember/v1/HScanRequest';
import type { HScanResponse as _ember_v1_HScanResponse, HScanResponse__Output as _ember_v1_HScanResponse__Output } from '../../ember/v1/HScanResponse';
import type { HSetRequest as _ember_v1_HSetRequest, HSetRequest__Output as _ember_v1_HSetRequest__Output } from '../../ember/v1/HSetRequest';
import type { HValsRequest as _ember_v1_HValsRequest, HValsRequest__Output as _ember_v1_HValsRequest__Output } from '../../ember/v1/HValsRequest';
import type { HashResponse as _ember_v1_HashResponse, HashResponse__Output as _ember_v1_HashResponse__Output } from '../../ember/v1/HashResponse';
import type { IncrByFloatRequest as _ember_v1_IncrByFloatRequest, IncrByFloatRequest__Output as _ember_v1_IncrByFloatRequest__Output } from '../../ember/v1/IncrByFloatRequest';
import type { IncrByRequest as _ember_v1_IncrByRequest, IncrByRequest__Output as _ember_v1_IncrByRequest__Output } from '../../ember/v1/IncrByRequest';
import type { IncrRequest as _ember_v1_IncrRequest, IncrRequest__Output as _ember_v1_IncrRequest__Output } from '../../ember/v1/IncrRequest';
import type { InfoRequest as _ember_v1_InfoRequest, InfoRequest__Output as _ember_v1_InfoRequest__Output } from '../../ember/v1/InfoRequest';
import type { InfoResponse as _ember_v1_InfoResponse, InfoResponse__Output as _ember_v1_InfoResponse__Output } from '../../ember/v1/InfoResponse';
import type { IntResponse as _ember_v1_IntResponse, IntResponse__Output as _ember_v1_IntResponse__Output } from '../../ember/v1/IntResponse';
import type { KeysRequest as _ember_v1_KeysRequest, KeysRequest__Output as _ember_v1_KeysRequest__Output } from '../../ember/v1/KeysRequest';
import type { KeysResponse as _ember_v1_KeysResponse, KeysResponse__Output as _ember_v1_KeysResponse__Output } from '../../ember/v1/KeysResponse';
import type { LIndexRequest as _ember_v1_LIndexRequest, LIndexRequest__Output as _ember_v1_LIndexRequest__Output } from '../../ember/v1/LIndexRequest';
import type { LInsertRequest as _ember_v1_LInsertRequest, LInsertRequest__Output as _ember_v1_LInsertRequest__Output } from '../../ember/v1/LInsertRequest';
import type { LLenRequest as _ember_v1_LLenRequest, LLenRequest__Output as _ember_v1_LLenRequest__Output } from '../../ember/v1/LLenRequest';
import type { LMoveRequest as _ember_v1_LMoveRequest, LMoveRequest__Output as _ember_v1_LMoveRequest__Output } from '../../ember/v1/LMoveRequest';
import type { LPopRequest as _ember_v1_LPopRequest, LPopRequest__Output as _ember_v1_LPopRequest__Output } from '../../ember/v1/LPopRequest';
import type { LPosRequest as _ember_v1_LPosRequest, LPosRequest__Output as _ember_v1_LPosRequest__Output } from '../../ember/v1/LPosRequest';
import type { LPushRequest as _ember_v1_LPushRequest, LPushRequest__Output as _ember_v1_LPushRequest__Output } from '../../ember/v1/LPushRequest';
import type { LRangeRequest as _ember_v1_LRangeRequest, LRangeRequest__Output as _ember_v1_LRangeRequest__Output } from '../../ember/v1/LRangeRequest';
import type { LRemRequest as _ember_v1_LRemRequest, LRemRequest__Output as _ember_v1_LRemRequest__Output } from '../../ember/v1/LRemRequest';
import type { LSetRequest as _ember_v1_LSetRequest, LSetRequest__Output as _ember_v1_LSetRequest__Output } from '../../ember/v1/LSetRequest';
import type { LTrimRequest as _ember_v1_LTrimRequest, LTrimRequest__Output as _ember_v1_LTrimRequest__Output } from '../../ember/v1/LTrimRequest';
import type { LastSaveRequest as _ember_v1_LastSaveRequest, LastSaveRequest__Output as _ember_v1_LastSaveRequest__Output } from '../../ember/v1/LastSaveRequest';
import type { MGetRequest as _ember_v1_MGetRequest, MGetRequest__Output as _ember_v1_MGetRequest__Output } from '../../ember/v1/MGetRequest';
import type { MGetResponse as _ember_v1_MGetResponse, MGetResponse__Output as _ember_v1_MGetResponse__Output } from '../../ember/v1/MGetResponse';
import type { MSetRequest as _ember_v1_MSetRequest, MSetRequest__Output as _ember_v1_MSetRequest__Output } from '../../ember/v1/MSetRequest';
import type { MSetResponse as _ember_v1_MSetResponse, MSetResponse__Output as _ember_v1_MSetResponse__Output } from '../../ember/v1/MSetResponse';
import type { OptionalArrayResponse as _ember_v1_OptionalArrayResponse, OptionalArrayResponse__Output as _ember_v1_OptionalArrayResponse__Output } from '../../ember/v1/OptionalArrayResponse';
import type { OptionalFloatResponse as _ember_v1_OptionalFloatResponse, OptionalFloatResponse__Output as _ember_v1_OptionalFloatResponse__Output } from '../../ember/v1/OptionalFloatResponse';
import type { OptionalIntResponse as _ember_v1_OptionalIntResponse, OptionalIntResponse__Output as _ember_v1_OptionalIntResponse__Output } from '../../ember/v1/OptionalIntResponse';
import type { PExpireRequest as _ember_v1_PExpireRequest, PExpireRequest__Output as _ember_v1_PExpireRequest__Output } from '../../ember/v1/PExpireRequest';
import type { PTtlRequest as _ember_v1_PTtlRequest, PTtlRequest__Output as _ember_v1_PTtlRequest__Output } from '../../ember/v1/PTtlRequest';
import type { PersistRequest as _ember_v1_PersistRequest, PersistRequest__Output as _ember_v1_PersistRequest__Output } from '../../ember/v1/PersistRequest';
import type { PingRequest as _ember_v1_PingRequest, PingRequest__Output as _ember_v1_PingRequest__Output } from '../../ember/v1/PingRequest';
import type { PingResponse as _ember_v1_PingResponse, PingResponse__Output as _ember_v1_PingResponse__Output } from '../../ember/v1/PingResponse';
import type { PipelineRequest as _ember_v1_PipelineRequest, PipelineRequest__Output as _ember_v1_PipelineRequest__Output } from '../../ember/v1/PipelineRequest';
import type { PipelineResponse as _ember_v1_PipelineResponse, PipelineResponse__Output as _ember_v1_PipelineResponse__Output } from '../../ember/v1/PipelineResponse';
import type { PubSubChannelsRequest as _ember_v1_PubSubChannelsRequest, PubSubChannelsRequest__Output as _ember_v1_PubSubChannelsRequest__Output } from '../../ember/v1/PubSubChannelsRequest';
import type { PubSubNumPatRequest as _ember_v1_PubSubNumPatRequest, PubSubNumPatRequest__Output as _ember_v1_PubSubNumPatRequest__Output } from '../../ember/v1/PubSubNumPatRequest';
import type { PubSubNumSubRequest as _ember_v1_PubSubNumSubRequest, PubSubNumSubRequest__Output as _ember_v1_PubSubNumSubRequest__Output } from '../../ember/v1/PubSubNumSubRequest';
import type { PubSubNumSubResponse as _ember_v1_PubSubNumSubResponse, PubSubNumSubResponse__Output as _ember_v1_PubSubNumSubResponse__Output } from '../../ember/v1/PubSubNumSubResponse';
import type { PublishRequest as _ember_v1_PublishRequest, PublishRequest__Output as _ember_v1_PublishRequest__Output } from '../../ember/v1/PublishRequest';
import type { RPopRequest as _ember_v1_RPopRequest, RPopRequest__Output as _ember_v1_RPopRequest__Output } from '../../ember/v1/RPopRequest';
import type { RPushRequest as _ember_v1_RPushRequest, RPushRequest__Output as _ember_v1_RPushRequest__Output } from '../../ember/v1/RPushRequest';
import type { RandomKeyRequest as _ember_v1_RandomKeyRequest, RandomKeyRequest__Output as _ember_v1_RandomKeyRequest__Output } from '../../ember/v1/RandomKeyRequest';
import type { RenameRequest as _ember_v1_RenameRequest, RenameRequest__Output as _ember_v1_RenameRequest__Output } from '../../ember/v1/RenameRequest';
import type { SAddRequest as _ember_v1_SAddRequest, SAddRequest__Output as _ember_v1_SAddRequest__Output } from '../../ember/v1/SAddRequest';
import type { SCardRequest as _ember_v1_SCardRequest, SCardRequest__Output as _ember_v1_SCardRequest__Output } from '../../ember/v1/SCardRequest';
import type { SDiffRequest as _ember_v1_SDiffRequest, SDiffRequest__Output as _ember_v1_SDiffRequest__Output } from '../../ember/v1/SDiffRequest';
import type { SDiffStoreRequest as _ember_v1_SDiffStoreRequest, SDiffStoreRequest__Output as _ember_v1_SDiffStoreRequest__Output } from '../../ember/v1/SDiffStoreRequest';
import type { SInterRequest as _ember_v1_SInterRequest, SInterRequest__Output as _ember_v1_SInterRequest__Output } from '../../ember/v1/SInterRequest';
import type { SInterStoreRequest as _ember_v1_SInterStoreRequest, SInterStoreRequest__Output as _ember_v1_SInterStoreRequest__Output } from '../../ember/v1/SInterStoreRequest';
import type { SIsMemberRequest as _ember_v1_SIsMemberRequest, SIsMemberRequest__Output as _ember_v1_SIsMemberRequest__Output } from '../../ember/v1/SIsMemberRequest';
import type { SMembersRequest as _ember_v1_SMembersRequest, SMembersRequest__Output as _ember_v1_SMembersRequest__Output } from '../../ember/v1/SMembersRequest';
import type { SMisMemberRequest as _ember_v1_SMisMemberRequest, SMisMemberRequest__Output as _ember_v1_SMisMemberRequest__Output } from '../../ember/v1/SMisMemberRequest';
import type { SPopRequest as _ember_v1_SPopRequest, SPopRequest__Output as _ember_v1_SPopRequest__Output } from '../../ember/v1/SPopRequest';
import type { SRandMemberRequest as _ember_v1_SRandMemberRequest, SRandMemberRequest__Output as _ember_v1_SRandMemberRequest__Output } from '../../ember/v1/SRandMemberRequest';
import type { SRemRequest as _ember_v1_SRemRequest, SRemRequest__Output as _ember_v1_SRemRequest__Output } from '../../ember/v1/SRemRequest';
import type { SScanRequest as _ember_v1_SScanRequest, SScanRequest__Output as _ember_v1_SScanRequest__Output } from '../../ember/v1/SScanRequest';
import type { SScanResponse as _ember_v1_SScanResponse, SScanResponse__Output as _ember_v1_SScanResponse__Output } from '../../ember/v1/SScanResponse';
import type { SUnionRequest as _ember_v1_SUnionRequest, SUnionRequest__Output as _ember_v1_SUnionRequest__Output } from '../../ember/v1/SUnionRequest';
import type { SUnionStoreRequest as _ember_v1_SUnionStoreRequest, SUnionStoreRequest__Output as _ember_v1_SUnionStoreRequest__Output } from '../../ember/v1/SUnionStoreRequest';
import type { ScanRequest as _ember_v1_ScanRequest, ScanRequest__Output as _ember_v1_ScanRequest__Output } from '../../ember/v1/ScanRequest';
import type { ScanResponse as _ember_v1_ScanResponse, ScanResponse__Output as _ember_v1_ScanResponse__Output } from '../../ember/v1/ScanResponse';
import type { SetRangeRequest as _ember_v1_SetRangeRequest, SetRangeRequest__Output as _ember_v1_SetRangeRequest__Output } from '../../ember/v1/SetRangeRequest';
import type { SetRequest as _ember_v1_SetRequest, SetRequest__Output as _ember_v1_SetRequest__Output } from '../../ember/v1/SetRequest';
import type { SetResponse as _ember_v1_SetResponse, SetResponse__Output as _ember_v1_SetResponse__Output } from '../../ember/v1/SetResponse';
import type { SlowLogGetRequest as _ember_v1_SlowLogGetRequest, SlowLogGetRequest__Output as _ember_v1_SlowLogGetRequest__Output } from '../../ember/v1/SlowLogGetRequest';
import type { SlowLogGetResponse as _ember_v1_SlowLogGetResponse, SlowLogGetResponse__Output as _ember_v1_SlowLogGetResponse__Output } from '../../ember/v1/SlowLogGetResponse';
import type { SlowLogLenRequest as _ember_v1_SlowLogLenRequest, SlowLogLenRequest__Output as _ember_v1_SlowLogLenRequest__Output } from '../../ember/v1/SlowLogLenRequest';
import type { SlowLogResetRequest as _ember_v1_SlowLogResetRequest, SlowLogResetRequest__Output as _ember_v1_SlowLogResetRequest__Output } from '../../ember/v1/SlowLogResetRequest';
import type { StatusResponse as _ember_v1_StatusResponse, StatusResponse__Output as _ember_v1_StatusResponse__Output } from '../../ember/v1/StatusResponse';
import type { StrlenRequest as _ember_v1_StrlenRequest, StrlenRequest__Output as _ember_v1_StrlenRequest__Output } from '../../ember/v1/StrlenRequest';
import type { SubscribeEvent as _ember_v1_SubscribeEvent, SubscribeEvent__Output as _ember_v1_SubscribeEvent__Output } from '../../ember/v1/SubscribeEvent';
import type { SubscribeRequest as _ember_v1_SubscribeRequest, SubscribeRequest__Output as _ember_v1_SubscribeRequest__Output } from '../../ember/v1/SubscribeRequest';
import type { TimeRequest as _ember_v1_TimeRequest, TimeRequest__Output as _ember_v1_TimeRequest__Output } from '../../ember/v1/TimeRequest';
import type { TimeResponse as _ember_v1_TimeResponse, TimeResponse__Output as _ember_v1_TimeResponse__Output } from '../../ember/v1/TimeResponse';
import type { TouchRequest as _ember_v1_TouchRequest, TouchRequest__Output as _ember_v1_TouchRequest__Output } from '../../ember/v1/TouchRequest';
import type { TtlRequest as _ember_v1_TtlRequest, TtlRequest__Output as _ember_v1_TtlRequest__Output } from '../../ember/v1/TtlRequest';
import type { TtlResponse as _ember_v1_TtlResponse, TtlResponse__Output as _ember_v1_TtlResponse__Output } from '../../ember/v1/TtlResponse';
import type { TypeRequest as _ember_v1_TypeRequest, TypeRequest__Output as _ember_v1_TypeRequest__Output } from '../../ember/v1/TypeRequest';
import type { TypeResponse as _ember_v1_TypeResponse, TypeResponse__Output as _ember_v1_TypeResponse__Output } from '../../ember/v1/TypeResponse';
import type { UnlinkRequest as _ember_v1_UnlinkRequest, UnlinkRequest__Output as _ember_v1_UnlinkRequest__Output } from '../../ember/v1/UnlinkRequest';
import type { VAddBatchRequest as _ember_v1_VAddBatchRequest, VAddBatchRequest__Output as _ember_v1_VAddBatchRequest__Output } from '../../ember/v1/VAddBatchRequest';
import type { VAddRequest as _ember_v1_VAddRequest, VAddRequest__Output as _ember_v1_VAddRequest__Output } from '../../ember/v1/VAddRequest';
import type { VCardRequest as _ember_v1_VCardRequest, VCardRequest__Output as _ember_v1_VCardRequest__Output } from '../../ember/v1/VCardRequest';
import type { VDimRequest as _ember_v1_VDimRequest, VDimRequest__Output as _ember_v1_VDimRequest__Output } from '../../ember/v1/VDimRequest';
import type { VGetRequest as _ember_v1_VGetRequest, VGetRequest__Output as _ember_v1_VGetRequest__Output } from '../../ember/v1/VGetRequest';
import type { VGetResponse as _ember_v1_VGetResponse, VGetResponse__Output as _ember_v1_VGetResponse__Output } from '../../ember/v1/VGetResponse';
import type { VInfoRequest as _ember_v1_VInfoRequest, VInfoRequest__Output as _ember_v1_VInfoRequest__Output } from '../../ember/v1/VInfoRequest';
import type { VInfoResponse as _ember_v1_VInfoResponse, VInfoResponse__Output as _ember_v1_VInfoResponse__Output } from '../../ember/v1/VInfoResponse';
import type { VRemRequest as _ember_v1_VRemRequest, VRemRequest__Output as _ember_v1_VRemRequest__Output } from '../../ember/v1/VRemRequest';
import type { VSimRequest as _ember_v1_VSimRequest, VSimRequest__Output as _ember_v1_VSimRequest__Output } from '../../ember/v1/VSimRequest';
import type { VSimResponse as _ember_v1_VSimResponse, VSimResponse__Output as _ember_v1_VSimResponse__Output } from '../../ember/v1/VSimResponse';
import type { ZAddRequest as _ember_v1_ZAddRequest, ZAddRequest__Output as _ember_v1_ZAddRequest__Output } from '../../ember/v1/ZAddRequest';
import type { ZCardRequest as _ember_v1_ZCardRequest, ZCardRequest__Output as _ember_v1_ZCardRequest__Output } from '../../ember/v1/ZCardRequest';
import type { ZCountRequest as _ember_v1_ZCountRequest, ZCountRequest__Output as _ember_v1_ZCountRequest__Output } from '../../ember/v1/ZCountRequest';
import type { ZDiffRequest as _ember_v1_ZDiffRequest, ZDiffRequest__Output as _ember_v1_ZDiffRequest__Output } from '../../ember/v1/ZDiffRequest';
import type { ZIncrByRequest as _ember_v1_ZIncrByRequest, ZIncrByRequest__Output as _ember_v1_ZIncrByRequest__Output } from '../../ember/v1/ZIncrByRequest';
import type { ZInterRequest as _ember_v1_ZInterRequest, ZInterRequest__Output as _ember_v1_ZInterRequest__Output } from '../../ember/v1/ZInterRequest';
import type { ZPopMaxRequest as _ember_v1_ZPopMaxRequest, ZPopMaxRequest__Output as _ember_v1_ZPopMaxRequest__Output } from '../../ember/v1/ZPopMaxRequest';
import type { ZPopMinRequest as _ember_v1_ZPopMinRequest, ZPopMinRequest__Output as _ember_v1_ZPopMinRequest__Output } from '../../ember/v1/ZPopMinRequest';
import type { ZRangeByScoreRequest as _ember_v1_ZRangeByScoreRequest, ZRangeByScoreRequest__Output as _ember_v1_ZRangeByScoreRequest__Output } from '../../ember/v1/ZRangeByScoreRequest';
import type { ZRangeRequest as _ember_v1_ZRangeRequest, ZRangeRequest__Output as _ember_v1_ZRangeRequest__Output } from '../../ember/v1/ZRangeRequest';
import type { ZRangeResponse as _ember_v1_ZRangeResponse, ZRangeResponse__Output as _ember_v1_ZRangeResponse__Output } from '../../ember/v1/ZRangeResponse';
import type { ZRankRequest as _ember_v1_ZRankRequest, ZRankRequest__Output as _ember_v1_ZRankRequest__Output } from '../../ember/v1/ZRankRequest';
import type { ZRemRequest as _ember_v1_ZRemRequest, ZRemRequest__Output as _ember_v1_ZRemRequest__Output } from '../../ember/v1/ZRemRequest';
import type { ZRevRangeByScoreRequest as _ember_v1_ZRevRangeByScoreRequest, ZRevRangeByScoreRequest__Output as _ember_v1_ZRevRangeByScoreRequest__Output } from '../../ember/v1/ZRevRangeByScoreRequest';
import type { ZRevRangeRequest as _ember_v1_ZRevRangeRequest, ZRevRangeRequest__Output as _ember_v1_ZRevRangeRequest__Output } from '../../ember/v1/ZRevRangeRequest';
import type { ZRevRankRequest as _ember_v1_ZRevRankRequest, ZRevRankRequest__Output as _ember_v1_ZRevRankRequest__Output } from '../../ember/v1/ZRevRankRequest';
import type { ZScanRequest as _ember_v1_ZScanRequest, ZScanRequest__Output as _ember_v1_ZScanRequest__Output } from '../../ember/v1/ZScanRequest';
import type { ZScanResponse as _ember_v1_ZScanResponse, ZScanResponse__Output as _ember_v1_ZScanResponse__Output } from '../../ember/v1/ZScanResponse';
import type { ZScoreRequest as _ember_v1_ZScoreRequest, ZScoreRequest__Output as _ember_v1_ZScoreRequest__Output } from '../../ember/v1/ZScoreRequest';
import type { ZUnionRequest as _ember_v1_ZUnionRequest, ZUnionRequest__Output as _ember_v1_ZUnionRequest__Output } from '../../ember/v1/ZUnionRequest';

export interface EmberCacheDefinition {
  Append: MethodDefinition<_ember_v1_AppendRequest, _ember_v1_IntResponse, _ember_v1_AppendRequest__Output, _ember_v1_IntResponse__Output>
  BgRewriteAof: MethodDefinition<_ember_v1_BgRewriteAofRequest, _ember_v1_StatusResponse, _ember_v1_BgRewriteAofRequest__Output, _ember_v1_StatusResponse__Output>
  BgSave: MethodDefinition<_ember_v1_BgSaveRequest, _ember_v1_StatusResponse, _ember_v1_BgSaveRequest__Output, _ember_v1_StatusResponse__Output>
  Copy: MethodDefinition<_ember_v1_CopyRequest, _ember_v1_BoolResponse, _ember_v1_CopyRequest__Output, _ember_v1_BoolResponse__Output>
  DbSize: MethodDefinition<_ember_v1_DbSizeRequest, _ember_v1_IntResponse, _ember_v1_DbSizeRequest__Output, _ember_v1_IntResponse__Output>
  Decr: MethodDefinition<_ember_v1_DecrRequest, _ember_v1_IntResponse, _ember_v1_DecrRequest__Output, _ember_v1_IntResponse__Output>
  DecrBy: MethodDefinition<_ember_v1_DecrByRequest, _ember_v1_IntResponse, _ember_v1_DecrByRequest__Output, _ember_v1_IntResponse__Output>
  Del: MethodDefinition<_ember_v1_DelRequest, _ember_v1_DelResponse, _ember_v1_DelRequest__Output, _ember_v1_DelResponse__Output>
  Echo: MethodDefinition<_ember_v1_EchoRequest, _ember_v1_EchoResponse, _ember_v1_EchoRequest__Output, _ember_v1_EchoResponse__Output>
  Exists: MethodDefinition<_ember_v1_ExistsRequest, _ember_v1_IntResponse, _ember_v1_ExistsRequest__Output, _ember_v1_IntResponse__Output>
  Expire: MethodDefinition<_ember_v1_ExpireRequest, _ember_v1_BoolResponse, _ember_v1_ExpireRequest__Output, _ember_v1_BoolResponse__Output>
  FlushDb: MethodDefinition<_ember_v1_FlushDbRequest, _ember_v1_StatusResponse, _ember_v1_FlushDbRequest__Output, _ember_v1_StatusResponse__Output>
  Get: MethodDefinition<_ember_v1_GetRequest, _ember_v1_GetResponse, _ember_v1_GetRequest__Output, _ember_v1_GetResponse__Output>
  GetDel: MethodDefinition<_ember_v1_GetDelRequest, _ember_v1_GetResponse, _ember_v1_GetDelRequest__Output, _ember_v1_GetResponse__Output>
  GetEx: MethodDefinition<_ember_v1_GetExRequest, _ember_v1_GetResponse, _ember_v1_GetExRequest__Output, _ember_v1_GetResponse__Output>
  GetRange: MethodDefinition<_ember_v1_GetRangeRequest, _ember_v1_GetResponse, _ember_v1_GetRangeRequest__Output, _ember_v1_GetResponse__Output>
  HDel: MethodDefinition<_ember_v1_HDelRequest, _ember_v1_IntResponse, _ember_v1_HDelRequest__Output, _ember_v1_IntResponse__Output>
  HExists: MethodDefinition<_ember_v1_HExistsRequest, _ember_v1_BoolResponse, _ember_v1_HExistsRequest__Output, _ember_v1_BoolResponse__Output>
  HGet: MethodDefinition<_ember_v1_HGetRequest, _ember_v1_GetResponse, _ember_v1_HGetRequest__Output, _ember_v1_GetResponse__Output>
  HGetAll: MethodDefinition<_ember_v1_HGetAllRequest, _ember_v1_HashResponse, _ember_v1_HGetAllRequest__Output, _ember_v1_HashResponse__Output>
  HIncrBy: MethodDefinition<_ember_v1_HIncrByRequest, _ember_v1_IntResponse, _ember_v1_HIncrByRequest__Output, _ember_v1_IntResponse__Output>
  HKeys: MethodDefinition<_ember_v1_HKeysRequest, _ember_v1_KeysResponse, _ember_v1_HKeysRequest__Output, _ember_v1_KeysResponse__Output>
  HLen: MethodDefinition<_ember_v1_HLenRequest, _ember_v1_IntResponse, _ember_v1_HLenRequest__Output, _ember_v1_IntResponse__Output>
  HMGet: MethodDefinition<_ember_v1_HMGetRequest, _ember_v1_OptionalArrayResponse, _ember_v1_HMGetRequest__Output, _ember_v1_OptionalArrayResponse__Output>
  HScan: MethodDefinition<_ember_v1_HScanRequest, _ember_v1_HScanResponse, _ember_v1_HScanRequest__Output, _ember_v1_HScanResponse__Output>
  HSet: MethodDefinition<_ember_v1_HSetRequest, _ember_v1_IntResponse, _ember_v1_HSetRequest__Output, _ember_v1_IntResponse__Output>
  HVals: MethodDefinition<_ember_v1_HValsRequest, _ember_v1_ArrayResponse, _ember_v1_HValsRequest__Output, _ember_v1_ArrayResponse__Output>
  Incr: MethodDefinition<_ember_v1_IncrRequest, _ember_v1_IntResponse, _ember_v1_IncrRequest__Output, _ember_v1_IntResponse__Output>
  IncrBy: MethodDefinition<_ember_v1_IncrByRequest, _ember_v1_IntResponse, _ember_v1_IncrByRequest__Output, _ember_v1_IntResponse__Output>
  IncrByFloat: MethodDefinition<_ember_v1_IncrByFloatRequest, _ember_v1_FloatResponse, _ember_v1_IncrByFloatRequest__Output, _ember_v1_FloatResponse__Output>
  Info: MethodDefinition<_ember_v1_InfoRequest, _ember_v1_InfoResponse, _ember_v1_InfoRequest__Output, _ember_v1_InfoResponse__Output>
  Keys: MethodDefinition<_ember_v1_KeysRequest, _ember_v1_KeysResponse, _ember_v1_KeysRequest__Output, _ember_v1_KeysResponse__Output>
  LIndex: MethodDefinition<_ember_v1_LIndexRequest, _ember_v1_GetResponse, _ember_v1_LIndexRequest__Output, _ember_v1_GetResponse__Output>
  LInsert: MethodDefinition<_ember_v1_LInsertRequest, _ember_v1_IntResponse, _ember_v1_LInsertRequest__Output, _ember_v1_IntResponse__Output>
  LLen: MethodDefinition<_ember_v1_LLenRequest, _ember_v1_IntResponse, _ember_v1_LLenRequest__Output, _ember_v1_IntResponse__Output>
  LMove: MethodDefinition<_ember_v1_LMoveRequest, _ember_v1_GetResponse, _ember_v1_LMoveRequest__Output, _ember_v1_GetResponse__Output>
  LPop: MethodDefinition<_ember_v1_LPopRequest, _ember_v1_GetResponse, _ember_v1_LPopRequest__Output, _ember_v1_GetResponse__Output>
  LPos: MethodDefinition<_ember_v1_LPosRequest, _ember_v1_OptionalIntResponse, _ember_v1_LPosRequest__Output, _ember_v1_OptionalIntResponse__Output>
  LPush: MethodDefinition<_ember_v1_LPushRequest, _ember_v1_IntResponse, _ember_v1_LPushRequest__Output, _ember_v1_IntResponse__Output>
  LRange: MethodDefinition<_ember_v1_LRangeRequest, _ember_v1_ArrayResponse, _ember_v1_LRangeRequest__Output, _ember_v1_ArrayResponse__Output>
  LRem: MethodDefinition<_ember_v1_LRemRequest, _ember_v1_IntResponse, _ember_v1_LRemRequest__Output, _ember_v1_IntResponse__Output>
  LSet: MethodDefinition<_ember_v1_LSetRequest, _ember_v1_StatusResponse, _ember_v1_LSetRequest__Output, _ember_v1_StatusResponse__Output>
  LTrim: MethodDefinition<_ember_v1_LTrimRequest, _ember_v1_StatusResponse, _ember_v1_LTrimRequest__Output, _ember_v1_StatusResponse__Output>
  LastSave: MethodDefinition<_ember_v1_LastSaveRequest, _ember_v1_IntResponse, _ember_v1_LastSaveRequest__Output, _ember_v1_IntResponse__Output>
  MGet: MethodDefinition<_ember_v1_MGetRequest, _ember_v1_MGetResponse, _ember_v1_MGetRequest__Output, _ember_v1_MGetResponse__Output>
  MSet: MethodDefinition<_ember_v1_MSetRequest, _ember_v1_MSetResponse, _ember_v1_MSetRequest__Output, _ember_v1_MSetResponse__Output>
  PExpire: MethodDefinition<_ember_v1_PExpireRequest, _ember_v1_BoolResponse, _ember_v1_PExpireRequest__Output, _ember_v1_BoolResponse__Output>
  PTtl: MethodDefinition<_ember_v1_PTtlRequest, _ember_v1_TtlResponse, _ember_v1_PTtlRequest__Output, _ember_v1_TtlResponse__Output>
  Persist: MethodDefinition<_ember_v1_PersistRequest, _ember_v1_BoolResponse, _ember_v1_PersistRequest__Output, _ember_v1_BoolResponse__Output>
  Ping: MethodDefinition<_ember_v1_PingRequest, _ember_v1_PingResponse, _ember_v1_PingRequest__Output, _ember_v1_PingResponse__Output>
  Pipeline: MethodDefinition<_ember_v1_PipelineRequest, _ember_v1_PipelineResponse, _ember_v1_PipelineRequest__Output, _ember_v1_PipelineResponse__Output>
  PubSubChannels: MethodDefinition<_ember_v1_PubSubChannelsRequest, _ember_v1_KeysResponse, _ember_v1_PubSubChannelsRequest__Output, _ember_v1_KeysResponse__Output>
  PubSubNumPat: MethodDefinition<_ember_v1_PubSubNumPatRequest, _ember_v1_IntResponse, _ember_v1_PubSubNumPatRequest__Output, _ember_v1_IntResponse__Output>
  PubSubNumSub: MethodDefinition<_ember_v1_PubSubNumSubRequest, _ember_v1_PubSubNumSubResponse, _ember_v1_PubSubNumSubRequest__Output, _ember_v1_PubSubNumSubResponse__Output>
  Publish: MethodDefinition<_ember_v1_PublishRequest, _ember_v1_IntResponse, _ember_v1_PublishRequest__Output, _ember_v1_IntResponse__Output>
  RPop: MethodDefinition<_ember_v1_RPopRequest, _ember_v1_GetResponse, _ember_v1_RPopRequest__Output, _ember_v1_GetResponse__Output>
  RPush: MethodDefinition<_ember_v1_RPushRequest, _ember_v1_IntResponse, _ember_v1_RPushRequest__Output, _ember_v1_IntResponse__Output>
  RandomKey: MethodDefinition<_ember_v1_RandomKeyRequest, _ember_v1_GetResponse, _ember_v1_RandomKeyRequest__Output, _ember_v1_GetResponse__Output>
  Rename: MethodDefinition<_ember_v1_RenameRequest, _ember_v1_StatusResponse, _ember_v1_RenameRequest__Output, _ember_v1_StatusResponse__Output>
  SAdd: MethodDefinition<_ember_v1_SAddRequest, _ember_v1_IntResponse, _ember_v1_SAddRequest__Output, _ember_v1_IntResponse__Output>
  SCard: MethodDefinition<_ember_v1_SCardRequest, _ember_v1_IntResponse, _ember_v1_SCardRequest__Output, _ember_v1_IntResponse__Output>
  SDiff: MethodDefinition<_ember_v1_SDiffRequest, _ember_v1_KeysResponse, _ember_v1_SDiffRequest__Output, _ember_v1_KeysResponse__Output>
  SDiffStore: MethodDefinition<_ember_v1_SDiffStoreRequest, _ember_v1_IntResponse, _ember_v1_SDiffStoreRequest__Output, _ember_v1_IntResponse__Output>
  SInter: MethodDefinition<_ember_v1_SInterRequest, _ember_v1_KeysResponse, _ember_v1_SInterRequest__Output, _ember_v1_KeysResponse__Output>
  SInterStore: MethodDefinition<_ember_v1_SInterStoreRequest, _ember_v1_IntResponse, _ember_v1_SInterStoreRequest__Output, _ember_v1_IntResponse__Output>
  SIsMember: MethodDefinition<_ember_v1_SIsMemberRequest, _ember_v1_BoolResponse, _ember_v1_SIsMemberRequest__Output, _ember_v1_BoolResponse__Output>
  SMembers: MethodDefinition<_ember_v1_SMembersRequest, _ember_v1_KeysResponse, _ember_v1_SMembersRequest__Output, _ember_v1_KeysResponse__Output>
  SMisMember: MethodDefinition<_ember_v1_SMisMemberRequest, _ember_v1_BoolArrayResponse, _ember_v1_SMisMemberRequest__Output, _ember_v1_BoolArrayResponse__Output>
  SPop: MethodDefinition<_ember_v1_SPopRequest, _ember_v1_ArrayResponse, _ember_v1_SPopRequest__Output, _ember_v1_ArrayResponse__Output>
  SRandMember: MethodDefinition<_ember_v1_SRandMemberRequest, _ember_v1_ArrayResponse, _ember_v1_SRandMemberRequest__Output, _ember_v1_ArrayResponse__Output>
  SRem: MethodDefinition<_ember_v1_SRemRequest, _ember_v1_IntResponse, _ember_v1_SRemRequest__Output, _ember_v1_IntResponse__Output>
  SScan: MethodDefinition<_ember_v1_SScanRequest, _ember_v1_SScanResponse, _ember_v1_SScanRequest__Output, _ember_v1_SScanResponse__Output>
  SUnion: MethodDefinition<_ember_v1_SUnionRequest, _ember_v1_KeysResponse, _ember_v1_SUnionRequest__Output, _ember_v1_KeysResponse__Output>
  SUnionStore: MethodDefinition<_ember_v1_SUnionStoreRequest, _ember_v1_IntResponse, _ember_v1_SUnionStoreRequest__Output, _ember_v1_IntResponse__Output>
  Scan: MethodDefinition<_ember_v1_ScanRequest, _ember_v1_ScanResponse, _ember_v1_ScanRequest__Output, _ember_v1_ScanResponse__Output>
  Set: MethodDefinition<_ember_v1_SetRequest, _ember_v1_SetResponse, _ember_v1_SetRequest__Output, _ember_v1_SetResponse__Output>
  SetRange: MethodDefinition<_ember_v1_SetRangeRequest, _ember_v1_IntResponse, _ember_v1_SetRangeRequest__Output, _ember_v1_IntResponse__Output>
  SlowLogGet: MethodDefinition<_ember_v1_SlowLogGetRequest, _ember_v1_SlowLogGetResponse, _ember_v1_SlowLogGetRequest__Output, _ember_v1_SlowLogGetResponse__Output>
  SlowLogLen: MethodDefinition<_ember_v1_SlowLogLenRequest, _ember_v1_IntResponse, _ember_v1_SlowLogLenRequest__Output, _ember_v1_IntResponse__Output>
  SlowLogReset: MethodDefinition<_ember_v1_SlowLogResetRequest, _ember_v1_StatusResponse, _ember_v1_SlowLogResetRequest__Output, _ember_v1_StatusResponse__Output>
  Strlen: MethodDefinition<_ember_v1_StrlenRequest, _ember_v1_IntResponse, _ember_v1_StrlenRequest__Output, _ember_v1_IntResponse__Output>
  Subscribe: MethodDefinition<_ember_v1_SubscribeRequest, _ember_v1_SubscribeEvent, _ember_v1_SubscribeRequest__Output, _ember_v1_SubscribeEvent__Output>
  Time: MethodDefinition<_ember_v1_TimeRequest, _ember_v1_TimeResponse, _ember_v1_TimeRequest__Output, _ember_v1_TimeResponse__Output>
  Touch: MethodDefinition<_ember_v1_TouchRequest, _ember_v1_IntResponse, _ember_v1_TouchRequest__Output, _ember_v1_IntResponse__Output>
  Ttl: MethodDefinition<_ember_v1_TtlRequest, _ember_v1_TtlResponse, _ember_v1_TtlRequest__Output, _ember_v1_TtlResponse__Output>
  Type: MethodDefinition<_ember_v1_TypeRequest, _ember_v1_TypeResponse, _ember_v1_TypeRequest__Output, _ember_v1_TypeResponse__Output>
  Unlink: MethodDefinition<_ember_v1_UnlinkRequest, _ember_v1_DelResponse, _ember_v1_UnlinkRequest__Output, _ember_v1_DelResponse__Output>
  VAdd: MethodDefinition<_ember_v1_VAddRequest, _ember_v1_BoolResponse, _ember_v1_VAddRequest__Output, _ember_v1_BoolResponse__Output>
  VAddBatch: MethodDefinition<_ember_v1_VAddBatchRequest, _ember_v1_IntResponse, _ember_v1_VAddBatchRequest__Output, _ember_v1_IntResponse__Output>
  VCard: MethodDefinition<_ember_v1_VCardRequest, _ember_v1_IntResponse, _ember_v1_VCardRequest__Output, _ember_v1_IntResponse__Output>
  VDim: MethodDefinition<_ember_v1_VDimRequest, _ember_v1_IntResponse, _ember_v1_VDimRequest__Output, _ember_v1_IntResponse__Output>
  VGet: MethodDefinition<_ember_v1_VGetRequest, _ember_v1_VGetResponse, _ember_v1_VGetRequest__Output, _ember_v1_VGetResponse__Output>
  VInfo: MethodDefinition<_ember_v1_VInfoRequest, _ember_v1_VInfoResponse, _ember_v1_VInfoRequest__Output, _ember_v1_VInfoResponse__Output>
  VRem: MethodDefinition<_ember_v1_VRemRequest, _ember_v1_BoolResponse, _ember_v1_VRemRequest__Output, _ember_v1_BoolResponse__Output>
  VSim: MethodDefinition<_ember_v1_VSimRequest, _ember_v1_VSimResponse, _ember_v1_VSimRequest__Output, _ember_v1_VSimResponse__Output>
  ZAdd: MethodDefinition<_ember_v1_ZAddRequest, _ember_v1_IntResponse, _ember_v1_ZAddRequest__Output, _ember_v1_IntResponse__Output>
  ZCard: MethodDefinition<_ember_v1_ZCardRequest, _ember_v1_IntResponse, _ember_v1_ZCardRequest__Output, _ember_v1_IntResponse__Output>
  ZCount: MethodDefinition<_ember_v1_ZCountRequest, _ember_v1_IntResponse, _ember_v1_ZCountRequest__Output, _ember_v1_IntResponse__Output>
  ZDiff: MethodDefinition<_ember_v1_ZDiffRequest, _ember_v1_ZRangeResponse, _ember_v1_ZDiffRequest__Output, _ember_v1_ZRangeResponse__Output>
  ZIncrBy: MethodDefinition<_ember_v1_ZIncrByRequest, _ember_v1_FloatResponse, _ember_v1_ZIncrByRequest__Output, _ember_v1_FloatResponse__Output>
  ZInter: MethodDefinition<_ember_v1_ZInterRequest, _ember_v1_ZRangeResponse, _ember_v1_ZInterRequest__Output, _ember_v1_ZRangeResponse__Output>
  ZPopMax: MethodDefinition<_ember_v1_ZPopMaxRequest, _ember_v1_ZRangeResponse, _ember_v1_ZPopMaxRequest__Output, _ember_v1_ZRangeResponse__Output>
  ZPopMin: MethodDefinition<_ember_v1_ZPopMinRequest, _ember_v1_ZRangeResponse, _ember_v1_ZPopMinRequest__Output, _ember_v1_ZRangeResponse__Output>
  ZRange: MethodDefinition<_ember_v1_ZRangeRequest, _ember_v1_ZRangeResponse, _ember_v1_ZRangeRequest__Output, _ember_v1_ZRangeResponse__Output>
  ZRangeByScore: MethodDefinition<_ember_v1_ZRangeByScoreRequest, _ember_v1_ZRangeResponse, _ember_v1_ZRangeByScoreRequest__Output, _ember_v1_ZRangeResponse__Output>
  ZRank: MethodDefinition<_ember_v1_ZRankRequest, _ember_v1_OptionalIntResponse, _ember_v1_ZRankRequest__Output, _ember_v1_OptionalIntResponse__Output>
  ZRem: MethodDefinition<_ember_v1_ZRemRequest, _ember_v1_IntResponse, _ember_v1_ZRemRequest__Output, _ember_v1_IntResponse__Output>
  ZRevRange: MethodDefinition<_ember_v1_ZRevRangeRequest, _ember_v1_ZRangeResponse, _ember_v1_ZRevRangeRequest__Output, _ember_v1_ZRangeResponse__Output>
  ZRevRangeByScore: MethodDefinition<_ember_v1_ZRevRangeByScoreRequest, _ember_v1_ZRangeResponse, _ember_v1_ZRevRangeByScoreRequest__Output, _ember_v1_ZRangeResponse__Output>
  ZRevRank: MethodDefinition<_ember_v1_ZRevRankRequest, _ember_v1_OptionalIntResponse, _ember_v1_ZRevRankRequest__Output, _ember_v1_OptionalIntResponse__Output>
  ZScan: MethodDefinition<_ember_v1_ZScanRequest, _ember_v1_ZScanResponse, _ember_v1_ZScanRequest__Output, _ember_v1_ZScanResponse__Output>
  ZScore: MethodDefinition<_ember_v1_ZScoreRequest, _ember_v1_OptionalFloatResponse, _ember_v1_ZScoreRequest__Output, _ember_v1_OptionalFloatResponse__Output>
  ZUnion: MethodDefinition<_ember_v1_ZUnionRequest, _ember_v1_ZRangeResponse, _ember_v1_ZUnionRequest__Output, _ember_v1_ZRangeResponse__Output>
}
