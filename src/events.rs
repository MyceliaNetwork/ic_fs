use ic_cdk::export::Principal;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ControllerAdded {
    pub(crate) controller: Principal,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ControllerRemoved {
    pub(crate) controller: Principal,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SubscriberAdded {
    pub(crate) subscriber: Principal,
    pub(crate) offset: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SubscriberRemoved {
    pub(crate) subscriber: Principal,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SubscriberOffsetModified {
    pub(crate) subscriber: Principal,
    pub(crate) offset: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum EventFilesystemEvent {
    ControllerAdded(ControllerAdded),
    ControllerRemoved(ControllerRemoved),
    SubscriberAdded(SubscriberAdded),
    SubscriberRemoved(SubscriberRemoved),
    SubscriberOffsetModified(SubscriberOffsetModified),
}