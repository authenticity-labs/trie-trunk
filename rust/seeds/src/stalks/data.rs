use super::*;
use crate::{Batch, OwnedBatchReader, Cursor, Cell};
use pil_codec::{Value, api::NativeValue};
use ciborium::Value as Data;

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum State { Absent=0, Null=1, Present=2 }

/// Variable-length columns retain one owned byte pool, never borrowed source
/// strings. Growth admits both old and new buffers before copying/releasing.
pub struct Bytes { spans:Column<(usize,usize)>, data:Vec<u8>, memory:MemoryLease }
impl Bytes {
    fn new(rows:usize,budget:&MemoryBudget)->Result<Self,Error> {
        Ok(Self {spans:Column::new(rows,budget)?,data:Vec::new(),memory:budget.reserve(0)?})
    }
    fn push(&mut self,value:&[u8],budget:&MemoryBudget)->Result<(),Error> {
        let start=self.data.len();let end=start.checked_add(value.len()).ok_or(Error::Overflow)?;
        if end>self.data.capacity() {
            let capacity=end.checked_next_power_of_two().ok_or(Error::Overflow)?;
            let memory=budget.reserve(capacity)?;
            let mut data=Vec::new();data.try_reserve_exact(capacity).map_err(|_|error("column byte allocation failed"))?;
            if data.capacity()!=capacity {return Err(error("column byte allocation exceeded reservation"));}
            data.extend_from_slice(&self.data);self.data=data;self.memory=memory;
        }
        self.data.extend_from_slice(value);self.spans.push((start,end))
    }
    pub fn get(&self,row:usize)->Option<&[u8]> {self.spans.values().get(row).map(|&(a,b)|&self.data[a..b])}
    fn bytes(&self)->usize {self.spans.bytes()+self.data.capacity()}
}
pub enum Values {
    Empty, Unsigned(Column<u64>), Signed(Column<i64>), WideUnsigned(Column<u128>), WideSigned(Column<i128>),
    Float(Column<f64>), Boolean(Column<u8>), Text(Bytes), Binary(Bytes),
}
impl Values {
    fn new(value:&Value<'_>,rows:usize,budget:&MemoryBudget)->Result<Self,Error> {
        Ok(match value {
            Value::U8(_)|Value::U16(_)|Value::U24(_)|Value::U32(_)|Value::U64(_)=>Self::Unsigned(Column::new(rows,budget)?),
            Value::I8(_)|Value::I16(_)|Value::I32(_)|Value::I64(_)|Value::TimestampMs(_)|Value::TimestampUs(_)|Value::TimestampNs(_)=>Self::Signed(Column::new(rows,budget)?),
            Value::U128(_)=>Self::WideUnsigned(Column::new(rows,budget)?),Value::I128(_)=>Self::WideSigned(Column::new(rows,budget)?),
            Value::F32(_)|Value::F64(_)=>Self::Float(Column::new(rows,budget)?),Value::Bool(_)=>Self::Boolean(Column::new(rows,budget)?),
            Value::String(_)=>Self::Text(Bytes::new(rows,budget)?),Value::Bytes(_)|Value::Uuid(_)=>Self::Binary(Bytes::new(rows,budget)?),
            _=>return Err(error("Stalk columns require projected scalar fields; select or flatten composite fields first")),
        })
    }
    fn empty(&mut self,budget:&MemoryBudget)->Result<(),Error> {
        match self {Self::Empty=>Ok(()),Self::Unsigned(v)=>v.push(0),Self::Signed(v)=>v.push(0),Self::WideUnsigned(v)=>v.push(0),Self::WideSigned(v)=>v.push(0),
            Self::Float(v)=>v.push(0.0),Self::Boolean(v)=>v.push(0),Self::Text(v)|Self::Binary(v)=>v.push(&[],budget)}
    }
    fn push(&mut self,value:Value<'_>,budget:&MemoryBudget)->Result<(),Error> {
        match (self,value) {
            (Self::Unsigned(c),Value::U8(v))=>c.push(v.into()),(Self::Unsigned(c),Value::U16(v))=>c.push(v.into()),
            (Self::Unsigned(c),Value::U24(v)|Value::U32(v))=>c.push(v.into()),(Self::Unsigned(c),Value::U64(v))=>c.push(v),
            (Self::Signed(c),Value::I8(v))=>c.push(v.into()),(Self::Signed(c),Value::I16(v))=>c.push(v.into()),(Self::Signed(c),Value::I32(v))=>c.push(v.into()),
            (Self::Signed(c),Value::I64(v)|Value::TimestampMs(v)|Value::TimestampUs(v)|Value::TimestampNs(v))=>c.push(v),
            (Self::WideUnsigned(c),Value::U128(v))=>c.push(v),(Self::WideSigned(c),Value::I128(v))=>c.push(v),
            (Self::Float(c),Value::F32(v))=>c.push(v.into()),(Self::Float(c),Value::F64(v))=>c.push(v),
            (Self::Boolean(c),Value::Bool(v))=>c.push(v.into()),(Self::Text(c),Value::String(v))=>c.push(v.as_bytes(),budget),
            (Self::Binary(c),Value::Bytes(v))=>c.push(v,budget),(Self::Binary(c),Value::Uuid(v))=>c.push(&v,budget),
            _=>Err(error("incompatible values in a Stalk column")),
        }
    }
    fn cell(&self,row:usize)->Result<Data,Error> {
        let bounds=||error("Stalk row outside column");
        Ok(match self {
            Self::Empty=>return Err(bounds()),Self::Unsigned(c)=>(*c.values().get(row).ok_or_else(bounds)?).into(),
            Self::Signed(c)=>(*c.values().get(row).ok_or_else(bounds)?).into(),Self::WideUnsigned(c)=>pil_codec::native_unsigned(*c.values().get(row).ok_or_else(bounds)?),
            Self::WideSigned(c)=>pil_codec::native_signed(*c.values().get(row).ok_or_else(bounds)?),Self::Float(c)=>(*c.values().get(row).ok_or_else(bounds)?).into(),
            Self::Boolean(c)=>Data::Bool(*c.values().get(row).ok_or_else(bounds)?!=0),
            Self::Text(c)=>Data::Text(std::str::from_utf8(c.get(row).ok_or_else(bounds)?).map_err(|_|error("invalid column text"))?.to_owned()),
            Self::Binary(c)=>Data::Bytes(c.get(row).ok_or_else(bounds)?.to_vec()),
        })
    }
    fn bytes(&self)->usize {match self {Self::Empty=>0,Self::Unsigned(c)=>c.bytes(),Self::Signed(c)=>c.bytes(),Self::WideUnsigned(c)=>c.bytes(),Self::WideSigned(c)=>c.bytes(),Self::Float(c)=>c.bytes(),Self::Boolean(c)=>c.bytes(),Self::Text(c)|Self::Binary(c)=>c.bytes()}}
}
// Two bits preserve absent/null/present without a byte per projected cell.
struct States {bytes:Option<Column<u8>>,uniform:u8,len:usize,capacity:usize}
impl States {
    fn new(capacity:usize,_budget:&MemoryBudget)->Result<Self,Error>{Ok(Self{bytes:None,uniform:0,len:0,capacity})}
    fn push(&mut self,value:u8,budget:&MemoryBudget)->Result<(),Error>{
        if self.len==self.capacity{return Err(error("Stalk state capacity exceeded"));}
        if self.len==0 {self.uniform=value;}
        if self.bytes.is_none() && value!=self.uniform {
            let mut bytes=Column::new(self.capacity.div_ceil(4),budget)?;
            for _ in 0..self.capacity.div_ceil(4) {bytes.push(self.uniform*0x55)?;}
            self.bytes=Some(bytes);
        }
        if let Some(bytes)=&mut self.bytes {let shift=(self.len%4)*2;bytes.values[self.len/4]=(bytes.values[self.len/4]&!(3<<shift))|(value<<shift);}
        self.len+=1;Ok(())
    }
    fn get(&self,row:usize)->Option<u8>{(row<self.len).then(||self.bytes.as_ref().map_or(self.uniform,|bytes|(bytes.values()[row/4]>>((row%4)*2))&3))}
    fn len(&self)->usize{self.len}
    fn bytes(&self)->usize{self.bytes.as_ref().map_or(0,Column::bytes)}
}
pub struct Field { name:String, states:States, values:Values, capacity:usize, kind:Option<pil_codec::Scalar> }
impl Field {
    pub fn name(&self)->&str {&self.name}
    pub fn values(&self)->&Values {&self.values}
    pub fn state(&self,row:usize)->Result<State,Error> {
        Ok(match self.states.get(row) {Some(0)=>State::Absent,Some(1)=>State::Null,Some(2)=>State::Present,_=>return Err(error("Stalk row outside column"))})
    }
    pub fn get(&self,row:usize)->Result<Cell,Error> {Ok(match self.state(row)? {State::Absent=>Cell::Absent,State::Null=>Cell::Value(Data::Null),State::Present=>Cell::Value(self.values.cell(row)?)})}
    fn push(&mut self,value:NativeValue<'_>,budget:&MemoryBudget)->Result<(),Error> {
        // Enum projections arrive as owned semantic scalars from RX. Keep
        // their declared numeric category; never recast based on the value.
        if let NativeValue::Composite(value)=value {
            use pil_codec::Scalar;
            let scalar=match &value {
                Data::Null=>Value::Null,Data::Bool(v)=>Value::Bool(*v),Data::Float(v)=>Value::F64(*v),Data::Text(v)=>Value::String(v),Data::Bytes(v)=>Value::Bytes(v),
                Data::Integer(_)|Data::Tag(2|3,_)=>match self.kind {
                    Some(Scalar::U8|Scalar::U16|Scalar::U24|Scalar::U32|Scalar::U64)=>Value::U64(u64::try_from(pil_codec::unsigned_native(&value)?).map_err(|_|Error::Overflow)?),
                    Some(Scalar::Uint(_))=>Value::U128(pil_codec::unsigned_native(&value)?),
                    Some(Scalar::I8|Scalar::I16|Scalar::I32|Scalar::I64)=>Value::I64(i64::try_from(pil_codec::signed_native(&value)?).map_err(|_|Error::Overflow)?),
                    Some(Scalar::Int(_))=>Value::I128(pil_codec::signed_native(&value)?),
                    _=>return Err(error("Stalk integer requires a declared numeric category")),
                },
                _=>return Err(error("Stalk columns require projected scalar fields; select or flatten composite fields first")),
            };
            return self.push(NativeValue::Scalar(scalar),budget);
        }
        let NativeValue::Scalar(value)=value else {unreachable!()};
        match value {
            Value::Absent=>{self.values.empty(budget)?;self.states.push(0,budget)},Value::Null=>{self.values.empty(budget)?;self.states.push(1,budget)},
            value=>{
                if matches!(self.values,Values::Empty) {
                    self.values=Values::new(&value,self.capacity,budget)?;
                    for _ in 0..self.states.len() {self.values.empty(budget)?;}
                }
                self.values.push(value,budget)?;self.states.push(2,budget)
            }
        }
    }
}
/// A bounded result chunk preserves its projected shape. Different Definition
/// shapes remain distinct; no implicit cast, truncation or source aliasing.
pub struct Chunk { fields:Vec<Field>, rows:usize, metadata:usize, definition:std::sync::Arc<crate::SeedDefinition>, _memory:MemoryLease }
impl Chunk {
    fn new(names:impl Iterator<Item=impl AsRef<str>>+Clone,rows:usize,budget:&MemoryBudget,definition:std::sync::Arc<crate::SeedDefinition>)->Result<Self,Error> {
        let metadata=names.clone().try_fold(std::mem::size_of::<Self>(),|n,name|n.checked_add(std::mem::size_of::<Field>()+name.as_ref().len()).ok_or(Error::Overflow))?;
        let memory=budget.reserve(metadata)?;
        let fields=names.map(|name|Ok(Field{name:name.as_ref().to_owned(),states:States::new(rows,budget)?,values:Values::Empty,capacity:rows,kind:None})).collect::<Result<_,Error>>()?;
        Ok(Self {fields,rows:0,metadata,definition,_memory:memory})
    }
    pub fn fields(&self)->&[Field] {&self.fields}
    pub fn definition(&self)->&std::sync::Arc<crate::SeedDefinition> {&self.definition}
    pub fn column(&self,name:&str)->Option<&Field> {self.fields.iter().find(|f|f.name==name)}
    pub fn len(&self)->usize {self.rows}
    pub fn is_empty(&self)->bool {self.rows==0}
    fn bytes(&self)->usize {self.metadata+self.fields.iter().map(|f|f.states.bytes()+f.values.bytes()).sum::<usize>()}
    fn object(&self,row:usize)->Result<crate::Object,Error> {
        if row>=self.rows {return Err(error("Stalk row outside chunk"));}
        self.fields.iter().filter_map(|f|match f.get(row) {Ok(Cell::Absent)=>None,Ok(Cell::Value(v))=>Some(Ok((f.name.clone(),v))),Err(e)=>Some(Err(e))}).collect()
    }
}
pub struct Stalks { chunks:Vec<Chunk>, rows:usize, limit:usize, budget:MemoryBudget }
impl Stalks {
    pub fn new(budget:MemoryBudget,row_limit:usize)->Self {Self {chunks:Vec::new(),rows:0,limit:row_limit,budget}}
    pub fn len(&self)->usize {self.rows}
    pub fn is_empty(&self)->bool {self.rows==0}
    pub fn chunks(&self)->&[Chunk] {&self.chunks}
    pub fn bytes(&self)->usize {self.chunks.iter().map(Chunk::bytes).sum()}
    pub fn grow(&mut self,batch:Batch)->Result<(),Error> {self.load(batch.into_reader())}
    pub fn load(&mut self,mut reader:OwnedBatchReader)->Result<(),Error> {
        let count=reader.batch().len();
        if self.rows.checked_add(count).ok_or(Error::Overflow)?>self.limit {return Err(error("Stalk row budget exhausted"));}
        let mut staged=Vec::<Chunk>::new();let mut row=0;
        while row<count {
            let shape=reader.batch().shape(row)?;let mut end=row+1;
            while end<count && reader.batch().shape(end)?==shape {end+=1;}
            let view=reader.view(row)?;let names=view.fields().collect::<Vec<_>>();
            let mut chunk=Chunk::new(names.iter().copied(),end-row,&self.budget,view.definition().clone())?;
            for (i,field) in chunk.fields.iter_mut().enumerate(){field.kind=view.scalar_kind_at(i);}
            for at in row..end {
                let view=reader.view(at)?;
                for (i,field) in chunk.fields.iter_mut().enumerate() {field.push(view.native_at(i)?,&self.budget)?;}
                chunk.rows+=1;
            }
            staged.push(chunk);row=end;
        }
        self.rows+=count;self.chunks.extend(staged);
        // `reader` and all its retained source Pack/Box leases drop here.
        Ok(())
    }
    pub fn object(&self,mut row:usize)->Result<crate::Object,Error> {
        for chunk in &self.chunks {if row<chunk.rows {return chunk.object(row);}row-=chunk.rows;}
        Err(error("Stalk row outside result"))
    }
    /// Exact integer sums; float columns remain float. Cross-category numeric
    /// conversions require an explicit policy instead of silently losing bits.
    pub fn sum(&self,name:&str)->Result<Cell,Error> {
        enum Sum {None,U(u128),I(i128),F(f64)}
        let mut sum=Sum::None;
        for chunk in &self.chunks {let Some(field)=chunk.column(name) else {continue};
            for i in 0..chunk.rows {if field.state(i)?!=State::Present {continue;}
                sum=match (&field.values,sum) {
                    (Values::Unsigned(v),Sum::None)=>Sum::U(v.values()[i].into()),(Values::Unsigned(v),Sum::U(n))=>Sum::U(n.checked_add(v.values()[i].into()).ok_or(Error::Overflow)?),
                    (Values::WideUnsigned(v),Sum::None)=>Sum::U(v.values()[i]),(Values::WideUnsigned(v),Sum::U(n))=>Sum::U(n.checked_add(v.values()[i]).ok_or(Error::Overflow)?),
                    (Values::Signed(v),Sum::None)=>Sum::I(v.values()[i].into()),(Values::Signed(v),Sum::I(n))=>Sum::I(n.checked_add(v.values()[i].into()).ok_or(Error::Overflow)?),
                    (Values::WideSigned(v),Sum::None)=>Sum::I(v.values()[i]),(Values::WideSigned(v),Sum::I(n))=>Sum::I(n.checked_add(v.values()[i]).ok_or(Error::Overflow)?),
                    (Values::Float(v),Sum::None)=>Sum::F(v.values()[i]),(Values::Float(v),Sum::F(n))=>Sum::F(n+v.values()[i]),
                    _=>return Err(error("sum requires compatible numeric Stalk columns")),
                };
            }
        }
        Ok(Cell::Value(match sum {Sum::None=>Data::Null,Sum::U(v)=>pil_codec::native_unsigned(v),Sum::I(v)=>pil_codec::native_signed(v),Sum::F(v) if v.is_finite()=>Data::Float(v),Sum::F(_)=>return Err(error("nonfinite Stalk sum"))}))
    }
}
/// Cooperatively bounded conversion through the existing query evaluator.
/// No query, filtering, sorting or storage/replication algorithm is replaced.
pub struct Growth { cursor:Cursor, stalks:Option<Stalks>, done:bool }
impl Growth {
    pub fn new(cursor:Cursor,budget:MemoryBudget,row_limit:usize)->Self {Self {cursor,stalks:Some(Stalks::new(budget,row_limit)),done:false}}
    pub fn poll(&mut self,work:usize)->Result<bool,Error> {
        if self.done {return Ok(true);}
        let result=(||{
            let slice=self.cursor.poll(256,work)?;
            self.stalks.as_mut().ok_or_else(||error("Stalk growth was cancelled"))?.grow(slice.batch)?;
            self.done=slice.done;Ok(self.done)
        })();
        if result.is_err() {self.cancel();}result
    }
    pub fn cancel(&mut self) {self.cursor.abort();self.stalks=None;}
    pub fn len(&self)->usize {self.stalks.as_ref().map_or(0,Stalks::len)}
    pub fn bytes(&self)->usize {self.stalks.as_ref().map_or(0,Stalks::bytes)}
    pub fn is_complete(&self)->bool {self.done&&self.stalks.is_some()}
    pub fn finish(mut self)->Result<Stalks,Error> {
        if !self.done {return Err(error("Stalk growth is incomplete"));}
        self.stalks.take().ok_or_else(||error("Stalk growth was cancelled"))
    }
}

#[cfg(test)]
mod state_tests {
    use super::*;
    #[test]
    fn uniform_states_allocate_only_when_the_pattern_changes() {
        for uniform in 0..3 {
            let budget=MemoryBudget::new(1024);
            let mut states=States::new(259,&budget).unwrap();
            for _ in 0..129 {states.push(uniform,&budget).unwrap();}
            assert_eq!(budget.used(),0);
            let other=(uniform+1)%3;states.push(other,&budget).unwrap();
            for _ in 130..259 {states.push(uniform,&budget).unwrap();}
            for i in 0..259 {assert_eq!(states.get(i),Some(if i==129{other}else{uniform}));}
            assert_eq!(states.get(259),None);assert_eq!(budget.used(),65);
            assert!(states.push(0,&budget).is_err());drop(states);assert_eq!(budget.used(),0);
        }
    }
    #[test]
    fn failed_state_expansion_keeps_existing_values() {
        let budget=MemoryBudget::new(0);let mut states=States::new(10,&budget).unwrap();
        states.push(2,&budget).unwrap();assert!(states.push(1,&budget).is_err());
        assert_eq!(states.get(0),Some(2));assert_eq!(states.len(),1);
    }
}
