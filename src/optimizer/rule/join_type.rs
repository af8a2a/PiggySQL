use lazy_static::lazy_static;

use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
lazy_static! {
    static ref JOIN_TYPE_RULE: Pattern = {
        Pattern {
            predicate: |_| true,
            children: PatternChildrenPredicate::None,
        }
    };
}