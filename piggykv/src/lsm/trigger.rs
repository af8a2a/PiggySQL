use super::mem_table::{key_value_bytes_len, KeyValue};

pub(crate) trait Trigger {
    fn item_process(&mut self, item: &KeyValue);

    fn is_exceeded(&self) -> bool;

    fn reset(&mut self);
}

#[derive(Copy, Clone)]
pub(crate) struct CountTrigger {
    item_count: usize,
    threshold: usize,
}

impl Trigger for CountTrigger {
    fn item_process(&mut self, _item: &KeyValue) {
        self.item_count += 1;
    }

    fn is_exceeded(&self) -> bool {
        self.item_count >= self.threshold
    }

    fn reset(&mut self) {
        self.item_count = 0;
    }
}

#[derive(Copy, Clone)]
pub(crate) struct SizeOfMemTrigger {
    size_of_mem: usize,
    threshold: usize,
}

impl Trigger for SizeOfMemTrigger {
    fn item_process(&mut self, item: &KeyValue) {
        self.size_of_mem += key_value_bytes_len(item);
    }

    fn is_exceeded(&self) -> bool {
        self.size_of_mem >= self.threshold
    }

    fn reset(&mut self) {
        self.size_of_mem = 0;
    }
}

#[derive(Copy, Clone, Debug)]
pub enum TriggerType {
    Count,
    SizeOfMem,
}

pub(crate) struct TriggerFactory {}

impl TriggerFactory {
    pub(crate) fn create(trigger_type: TriggerType, threshold: usize) -> Box<dyn Trigger + Send> {
        match trigger_type {
            TriggerType::Count => Box::new(CountTrigger {
                item_count: 0,
                threshold,
            }),
            TriggerType::SizeOfMem => Box::new(SizeOfMemTrigger {
                size_of_mem: 0,
                threshold,
            }),
        }
    }
}
