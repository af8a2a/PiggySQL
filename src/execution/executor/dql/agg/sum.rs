use crate::errors::*;
use crate::expression::BinaryOperator;
use crate::types::value::{DataValue, ValueRef};
use crate::types::LogicalType;
use ahash::RandomState;
use std::collections::HashSet;
use std::sync::Arc;

use super::Accumulator;

pub struct SumAccumulator {
    result: DataValue,
}

impl SumAccumulator {
    pub fn new(ty: &LogicalType) -> Self {
        assert!(ty.is_numeric());

        Self {
            result: DataValue::init(ty),
        }
    }
}

impl Accumulator for SumAccumulator {
    fn update_value(&mut self, value: &ValueRef) -> Result<()> {
        if !value.is_null() {
            self.result = DataValue::binary_op(&self.result, value, &BinaryOperator::Plus)?;
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<ValueRef> {
        Ok(Arc::new(self.result.clone()))
    }
}

pub struct DistinctSumAccumulator {
    distinct_values: HashSet<ValueRef, RandomState>,
    inner: SumAccumulator,
}

impl DistinctSumAccumulator {
    pub fn new(ty: &LogicalType) -> Self {
        Self {
            distinct_values: HashSet::default(),
            inner: SumAccumulator::new(ty),
        }
    }
}

impl Accumulator for DistinctSumAccumulator {
    fn update_value(&mut self, value: &ValueRef) -> Result<()> {
        if !self.distinct_values.contains(value) {
            self.distinct_values.insert(value.clone());
            self.inner.update_value(value)?;
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<ValueRef> {
        self.inner.evaluate()
    }
}
