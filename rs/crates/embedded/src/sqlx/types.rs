// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::Antfly;
use either::Either;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use sqlx_core::{
    arguments::Arguments,
    column::{Column, ColumnIndex},
    decode::Decode,
    encode::{Encode, IsNull},
    error::{BoxDynError, Error},
    row::Row,
    sql_str::SqlStr,
    statement::Statement,
    type_info::TypeInfo,
    types::Type,
    value::{Value, ValueRef},
};
use std::{borrow::Cow, fmt, sync::Arc};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AntflyTypeInfo(pub String);
impl fmt::Display for AntflyTypeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl TypeInfo for AntflyTypeInfo {
    fn is_null(&self) -> bool {
        self.0 == "null"
    }
    fn name(&self) -> &str {
        &self.0
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AntflyColumn {
    #[serde(skip)]
    pub(crate) ordinal: usize,
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) kind: AntflyTypeInfo,
}
impl Column for AntflyColumn {
    type Database = Antfly;
    fn ordinal(&self) -> usize {
        self.ordinal
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn type_info(&self) -> &AntflyTypeInfo {
        &self.kind
    }
}
#[derive(Clone, Debug)]
pub struct AntflyValue {
    pub(crate) kind: AntflyTypeInfo,
    pub(crate) value: Json,
    pub(crate) null: bool,
}
#[derive(Clone, Copy, Debug)]
pub struct AntflyValueRef<'r>(pub(crate) &'r AntflyValue);
impl Value for AntflyValue {
    type Database = Antfly;
    fn as_ref(&self) -> AntflyValueRef<'_> {
        AntflyValueRef(self)
    }
    fn type_info(&self) -> Cow<'_, AntflyTypeInfo> {
        Cow::Borrowed(&self.kind)
    }
    fn is_null(&self) -> bool {
        self.null
    }
}
impl<'r> ValueRef<'r> for AntflyValueRef<'r> {
    type Database = Antfly;
    fn to_owned(&self) -> AntflyValue {
        self.0.clone()
    }
    fn type_info(&self) -> Cow<'_, AntflyTypeInfo> {
        Cow::Borrowed(&self.0.kind)
    }
    fn is_null(&self) -> bool {
        self.0.null
    }
}
#[derive(Debug, Clone)]
pub struct AntflyRow {
    pub(crate) columns: Arc<[AntflyColumn]>,
    pub(crate) values: Vec<AntflyValue>,
}
impl Row for AntflyRow {
    type Database = Antfly;
    fn columns(&self) -> &[AntflyColumn] {
        &self.columns
    }
    fn try_get_raw<I: ColumnIndex<Self>>(&self, index: I) -> Result<AntflyValueRef<'_>, Error> {
        Ok(AntflyValueRef(&self.values[index.index(self)?]))
    }
}
impl ColumnIndex<AntflyRow> for &str {
    fn index(&self, row: &AntflyRow) -> Result<usize, Error> {
        row.columns
            .iter()
            .position(|c| c.name == *self)
            .ok_or_else(|| Error::ColumnNotFound((*self).to_owned()))
    }
}
#[derive(Debug, Default, Clone)]
pub struct AntflyQueryResult {
    pub(crate) affected: u64,
}
impl AntflyQueryResult {
    pub fn rows_affected(&self) -> u64 {
        self.affected
    }
}
impl Extend<Self> for AntflyQueryResult {
    fn extend<T: IntoIterator<Item = Self>>(&mut self, iter: T) {
        for value in iter {
            self.affected += value.affected
        }
    }
}
#[derive(Default, Debug)]
pub struct AntflyArguments {
    pub(crate) values: Vec<Json>,
}
impl Arguments for AntflyArguments {
    type Database = Antfly;
    fn reserve(&mut self, additional: usize, _size: usize) {
        self.values.reserve(additional)
    }
    fn add<'t, T: Encode<'t, Antfly> + Type<Antfly>>(
        &mut self,
        value: T,
    ) -> Result<(), BoxDynError> {
        let before = self.values.len();
        match value.encode(&mut self.values) {
            Ok(IsNull::Yes) => {
                self.values.truncate(before);
                self.values.push(Json::Null);
                Ok(())
            }
            Ok(IsNull::No) => Ok(()),
            Err(error) => {
                self.values.truncate(before);
                Err(error)
            }
        }
    }
    fn len(&self) -> usize {
        self.values.len()
    }
    fn format_placeholder<W: fmt::Write>(&self, w: &mut W) -> fmt::Result {
        write!(w, "${}", self.values.len())
    }
}
sqlx_core::impl_into_arguments_for_arguments!(AntflyArguments);
sqlx_core::impl_encode_for_option!(Antfly);
#[derive(Clone, Debug)]
pub struct AntflyStatement {
    pub(crate) sql: SqlStr,
    pub(crate) columns: Vec<AntflyColumn>,
    pub(crate) parameters: Vec<AntflyTypeInfo>,
}
impl Statement for AntflyStatement {
    type Database = Antfly;
    fn into_sql(self) -> SqlStr {
        self.sql
    }
    fn sql(&self) -> &SqlStr {
        &self.sql
    }
    fn parameters(&self) -> Option<Either<&[AntflyTypeInfo], usize>> {
        Some(Either::Left(&self.parameters))
    }
    fn columns(&self) -> &[AntflyColumn] {
        &self.columns
    }
    sqlx_core::impl_statement_query!(AntflyArguments);
}
impl ColumnIndex<AntflyStatement> for &str {
    fn index(&self, statement: &AntflyStatement) -> Result<usize, Error> {
        statement
            .columns
            .iter()
            .position(|c| c.name == *self)
            .ok_or_else(|| Error::ColumnNotFound((*self).to_owned()))
    }
}
macro_rules! integer {($($ty:ty),*)=>{$(
 impl Type<Antfly> for $ty{fn type_info()->AntflyTypeInfo{AntflyTypeInfo("integer".into())}}
 impl<'q> Encode<'q,Antfly> for $ty{fn encode_by_ref(&self,buf:&mut Vec<Json>)->Result<IsNull,BoxDynError>{buf.push(Json::from(*self as i64));Ok(IsNull::No)}}
 impl<'r> Decode<'r,Antfly> for $ty{fn decode(value:AntflyValueRef<'r>)->Result<Self,BoxDynError>{let n=if let Some(n)=value.0.value.as_i64(){n}else{value.0.value.as_str().ok_or("expected integer")?.parse::<i64>()?};Ok(<$ty>::try_from(n)?)}}
 )*}}
integer!(i8, i16, i32, i64);
macro_rules! float {($($ty:ty),*)=>{$(
 impl Type<Antfly> for $ty{fn type_info()->AntflyTypeInfo{AntflyTypeInfo("number".into())}}
 impl<'q> Encode<'q,Antfly> for $ty{fn encode_by_ref(&self,buf:&mut Vec<Json>)->Result<IsNull,BoxDynError>{let n=serde_json::Number::from_f64(*self as f64).ok_or("non-finite number")?;buf.push(Json::Number(n));Ok(IsNull::No)}}
 impl<'r> Decode<'r,Antfly> for $ty{fn decode(value:AntflyValueRef<'r>)->Result<Self,BoxDynError>{Ok(value.0.value.as_f64().ok_or("expected number")? as $ty)}}
 )*}}
float!(f32, f64);
impl Type<Antfly> for bool {
    fn type_info() -> AntflyTypeInfo {
        AntflyTypeInfo("boolean".into())
    }
}
impl<'q> Encode<'q, Antfly> for bool {
    fn encode_by_ref(&self, buf: &mut Vec<Json>) -> Result<IsNull, BoxDynError> {
        buf.push(Json::Bool(*self));
        Ok(IsNull::No)
    }
}
impl<'r> Decode<'r, Antfly> for bool {
    fn decode(value: AntflyValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.0.value.as_bool().ok_or("expected boolean")?)
    }
}
impl Type<Antfly> for str {
    fn type_info() -> AntflyTypeInfo {
        AntflyTypeInfo("string".into())
    }
    fn compatible(kind: &AntflyTypeInfo) -> bool {
        matches!(kind.0.as_str(), "string" | "uuid" | "datetime")
    }
}
impl Type<Antfly> for String {
    fn type_info() -> AntflyTypeInfo {
        <str as Type<Antfly>>::type_info()
    }
    fn compatible(kind: &AntflyTypeInfo) -> bool {
        <str as Type<Antfly>>::compatible(kind)
    }
}
impl<'q> Encode<'q, Antfly> for str {
    fn encode_by_ref(&self, buf: &mut Vec<Json>) -> Result<IsNull, BoxDynError> {
        buf.push(Json::String(self.to_owned()));
        Ok(IsNull::No)
    }
}
impl<'q> Encode<'q, Antfly> for String {
    fn encode_by_ref(&self, buf: &mut Vec<Json>) -> Result<IsNull, BoxDynError> {
        self.as_str().encode_by_ref(buf)
    }
}
impl<'r> Decode<'r, Antfly> for &'r str {
    fn decode(value: AntflyValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.0.value.as_str().ok_or("expected string")?)
    }
}
impl<'r> Decode<'r, Antfly> for String {
    fn decode(value: AntflyValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(<&str as Decode<Antfly>>::decode(value)?.to_owned())
    }
}
impl Type<Antfly> for Json {
    fn type_info() -> AntflyTypeInfo {
        AntflyTypeInfo("json".into())
    }
}
impl<'q> Encode<'q, Antfly> for Json {
    fn encode_by_ref(&self, buf: &mut Vec<Json>) -> Result<IsNull, BoxDynError> {
        buf.push(self.clone());
        Ok(IsNull::No)
    }
}
impl<'r> Decode<'r, Antfly> for Json {
    fn decode(value: AntflyValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.0.value.clone())
    }
}
impl Type<Antfly> for [u8] {
    fn type_info() -> AntflyTypeInfo {
        AntflyTypeInfo("string".into())
    }
}
impl Type<Antfly> for Vec<u8> {
    fn type_info() -> AntflyTypeInfo {
        <[u8] as Type<Antfly>>::type_info()
    }
}
impl<'q> Encode<'q, Antfly> for [u8] {
    fn encode_by_ref(&self, buf: &mut Vec<Json>) -> Result<IsNull, BoxDynError> {
        std::str::from_utf8(self)?.encode_by_ref(buf)
    }
}
impl<'q> Encode<'q, Antfly> for Vec<u8> {
    fn encode_by_ref(&self, buf: &mut Vec<Json>) -> Result<IsNull, BoxDynError> {
        self.as_slice().encode_by_ref(buf)
    }
}
impl<'r> Decode<'r, Antfly> for Vec<u8> {
    fn decode(value: AntflyValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(<&str as Decode<Antfly>>::decode(value)?.as_bytes().to_vec())
    }
}

sqlx_core::impl_column_index_for_row!(AntflyRow);
sqlx_core::impl_column_index_for_statement!(AntflyStatement);
