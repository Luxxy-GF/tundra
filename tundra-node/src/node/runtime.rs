use super::{
    docker::{ContainerEvent, ContainerInfo, DockerAdapter},
    incus::IncusAdapter,
};
use futures::Stream;
use std::pin::Pin;

#[derive(Clone)]
pub enum RuntimeAdapter {
    Docker(DockerAdapter),
    Incus(IncusAdapter),
}

impl RuntimeAdapter {
    pub fn new() -> anyhow::Result<Self> {
        if let Some(socket) = std::env::var_os("WINGS_INCUS_SOCKET") {
            Ok(Self::Incus(IncusAdapter::new(socket.into())?))
        } else {
            Ok(Self::Docker(DockerAdapter::new()?))
        }
    }

    pub async fn inspect(&self, name: &str) -> anyhow::Result<ContainerInfo> {
        match self {
            Self::Docker(runtime) => runtime.inspect(name).await,
            Self::Incus(runtime) => runtime.inspect(name).await,
        }
    }

    pub fn events(&self) -> Pin<Box<dyn Stream<Item = ContainerEvent> + Send>> {
        match self {
            Self::Docker(runtime) => runtime.events(),
            Self::Incus(runtime) => runtime.events(),
        }
    }
}
