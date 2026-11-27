// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// CUTLASS is an offline compiler dependency; see the adjacent artifact license.
#pragma once
#include "cutlass/gemm/warp/mma_tensor_op.h"
#include "cutlass/gemm/warp/default_mma_tensor_op_sm80.h"
namespace cutlass {
namespace arch { struct OpGlinerCompensatedBF16 {}; }
namespace gemm { namespace warp {
template<typename W,typename LA,typename LB,typename LC,typename Policy,int P,bool Row>
class GlinerCompensatedBF16 : public MmaTensorOp<W,float,LA,float,LB,float,LC,Policy,P,Row> {
 using Base = MmaTensorOp<W,float,LA,float,LB,float,LC,Policy,P,Row>;
 static_assert(Policy::Operator::Shape::kK == 8, "GLiNER BF16x3 packing is qualified for k8 only");
 static_assert(PreferredRoundingMode<bfloat16_t,float>::kRound == FloatRoundStyle::round_to_nearest, "BF16x3 residuals require RNE");
public:
 // Reuse CUTLASS's staged FP32 accumulation for the three correction terms.
 using MathOperator = arch::OpMultiplyAddFastF32;
 using FragmentA = typename Base::FragmentA;
 using FragmentB = typename Base::FragmentB;
 using FragmentC = typename Base::FragmentC;
 using HA = typename Base::TransformedFragmentA;
 using HB = typename Base::TransformedFragmentB;
 using TransformedFragmentA = Array<bfloat16_t,2*HA::kElements>;
 using TransformedFragmentB = Array<bfloat16_t,2*HB::kElements>;
 CUTLASS_DEVICE GlinerCompensatedBF16():Base(){}
 CUTLASS_DEVICE void transform(TransformedFragmentA& a,TransformedFragmentB& b,FragmentA const& x,FragmentB const& w) const {
  HA *ap=reinterpret_cast<HA*>(&a);HB *bp=reinterpret_cast<HB*>(&b);
  Base::transform(ap[0],bp[0],x,w);
  FragmentA xr;FragmentB wr;
  NumericConverter<bfloat16_t,float,FloatRoundStyle::round_to_nearest> round;
  CUTLASS_PRAGMA_UNROLL
  for(int i=0;i<FragmentA::kElements;++i)xr[i]=x[i]-float(round(x[i]));
  CUTLASS_PRAGMA_UNROLL
  for(int i=0;i<FragmentB::kElements;++i)wr[i]=w[i]-float(round(w[i]));
  Base::transform(ap[1],bp[1],xr,wr);
 }
 CUTLASS_DEVICE void operator()(FragmentC& d,TransformedFragmentA const& a,TransformedFragmentB const& b,FragmentC const& c) const {
  HA const*ap=reinterpret_cast<HA const*>(&a);HB const*bp=reinterpret_cast<HB const*>(&b);
  Base::operator()(d,ap[1],bp[0],c);
  Base::operator()(d,ap[0],bp[1],d);
  Base::operator()(d,ap[0],bp[0],d);
 }
};
template<typename W,typename I,typename LA,typename LB,typename LC,int P,bool Row>
struct DefaultMmaTensorOp<W,I,float,LA,float,LB,float,LC,arch::OpGlinerCompensatedBF16,P,Row> {
 using Policy = MmaTensorOpPolicy<arch::Mma<I,32,bfloat16_t,layout::RowMajor,bfloat16_t,layout::ColumnMajor,float,layout::RowMajor,arch::OpMultiplyAdd>,MatrixShape<1,1>>;
 using Type = GlinerCompensatedBF16<W,LA,LB,LC,Policy,P,Row>;
};
}}}
