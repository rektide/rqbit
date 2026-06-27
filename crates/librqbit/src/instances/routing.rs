use std::collections::{HashMap, HashSet};

use parking_lot::RwLock;
use tracing::trace;

use crate::instances::InstanceId;

pub(crate) struct RoutingTable {
    info_hash_to_instances: RwLock<HashMap<[u8; 20], HashSet<InstanceId>>>,
}

impl RoutingTable {
    pub(crate) fn new() -> Self {
        Self {
            info_hash_to_instances: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn add(&self, info_hash: &[u8; 20], instance_id: InstanceId) {
        let mut g = self.info_hash_to_instances.write();
        g.entry(*info_hash).or_default().insert(instance_id.clone());
        trace!(?info_hash, %instance_id, "routing table: added");
    }

    pub(crate) fn add_many(&self, info_hashes: &[[u8; 20]], instance_id: &InstanceId) {
        let mut g = self.info_hash_to_instances.write();
        for ih in info_hashes {
            g.entry(*ih).or_default().insert(instance_id.clone());
        }
    }

    pub(crate) fn remove(&self, info_hash: &[u8; 20], instance_id: &InstanceId) {
        let mut g = self.info_hash_to_instances.write();
        if let Some(instances) = g.get_mut(info_hash) {
            instances.remove(instance_id);
            if instances.is_empty() {
                g.remove(info_hash);
            }
        }
    }

    pub(crate) fn remove_instance(&self, instance_id: &InstanceId) {
        let mut g = self.info_hash_to_instances.write();
        g.retain(|_, instances| {
            instances.remove(instance_id);
            !instances.is_empty()
        });
    }

    pub(crate) fn lookup(&self, info_hash: &[u8; 20]) -> Option<InstanceId> {
        let g = self.info_hash_to_instances.read();
        g.get(info_hash).and_then(|s| s.iter().next().cloned())
    }

    #[allow(dead_code)]
    pub(crate) fn snapshot(&self) -> Vec<[u8; 20]> {
        self.info_hash_to_instances.read().keys().copied().collect()
    }
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self::new()
    }
}
