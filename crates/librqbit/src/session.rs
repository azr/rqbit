use std::{
    fs::File, io::Read, net::SocketAddr, ops::Deref, path::PathBuf, str::FromStr, time::Duration, sync::{atomic::AtomicBool, Arc},
};

use anyhow::Context;
use buffers::ByteString;
use dht::{Dht, Id20, PersistentDht, PersistentDhtConfig};
use librqbit_core::{
    magnet::Magnet,
    peer_id::generate_peer_id,
    torrent_metainfo::{torrent_from_bytes, TorrentMetaV1, TorrentMetaV1Info, TorrentMetaV1Owned},
};
use log::{debug, info, warn};
use parking_lot::RwLock;
use tokio_stream::StreamExt;

use crate::{
    dht_utils::{read_metainfo_from_peer_receiver, ReadMetainfoResult},
    peer_connection::PeerConnectionOptions,
    spawn_utils::{spawn, BlockingSpawner},
    torrent_manager::{TorrentManagerBuilder, TorrentManagerHandle},
};

#[derive(Clone)]
pub enum ManagedTorrentState {
    Initializing,
    Running(TorrentManagerHandle),
}

#[derive(Clone)]
pub struct ManagedTorrent {
    pub info_hash: Id20,
    pub output_folder: PathBuf,
    pub state: ManagedTorrentState,
}

impl PartialEq for ManagedTorrent {
    fn eq(&self, other: &Self) -> bool {
        self.info_hash == other.info_hash && self.output_folder == other.output_folder
    }
}

#[derive(Default)]
pub struct SessionLocked {
    torrents: Vec<ManagedTorrent>,
}

enum SessionLockedAddTorrentResult {
    AlreadyManaged(ManagedTorrent),
    Added(usize),
}

pub enum StopTorrentResult {
    TorrentInitializing,
    NotFound,
    Removed(ManagedTorrent),
}

impl SessionLocked {
    fn add_torrent(&mut self, torrent: ManagedTorrent) -> SessionLockedAddTorrentResult {
        if let Some(handle) = self.torrents.iter().find(|t| **t == torrent) {
            return SessionLockedAddTorrentResult::AlreadyManaged(handle.clone());
        }
        let idx = self.torrents.len();
        self.torrents.push(torrent);
        SessionLockedAddTorrentResult::Added(idx)
    }
    fn stop_torrent(&mut self, info_hash: Id20) -> StopTorrentResult {
        if let Some(torrent) = self
            .torrents
            .clone()
            .iter()
            .find(|t| t.info_hash == info_hash.clone())
        {
            match &torrent.state {
                ManagedTorrentState::Initializing => {
                    StopTorrentResult::TorrentInitializing
                }
                ManagedTorrentState::Running(handle) => {
                    handle.cancel();
                    self.torrents.retain(|t| t.info_hash != info_hash.clone());
                    StopTorrentResult::Removed(torrent.deref().clone())
                }
            }
        } else {
            StopTorrentResult::NotFound
        }
    }
}

pub struct Session {
    peer_id: Id20,
    dht: Option<Dht>,
    peer_opts: PeerConnectionOptions,
    spawner: BlockingSpawner,
    locked: RwLock<SessionLocked>,
    output_folder: PathBuf,
}

async fn torrent_from_url(url: &str) -> anyhow::Result<TorrentMetaV1Owned> {
    let response = reqwest::get(url)
        .await
        .with_context(|| format!("error downloading torrent metadata from {url}"))?;
    if !response.status().is_success() {
        anyhow::bail!("GET {} returned {}", url, response.status())
    }
    let b = response
        .bytes()
        .await
        .with_context(|| format!("error reading repsonse body from {url}"))?;
    torrent_from_bytes(&b).context("error decoding torrent")
}

fn torrent_from_file(filename: &str) -> anyhow::Result<TorrentMetaV1Owned> {
    let mut buf = Vec::new();
    if filename == "-" {
        std::io::stdin()
            .read_to_end(&mut buf)
            .context("error reading stdin")?;
    } else {
        File::open(filename)
            .with_context(|| format!("error opening {filename}"))?
            .read_to_end(&mut buf)
            .with_context(|| format!("error reading {filename}"))?;
    }
    torrent_from_bytes(&buf).context("error decoding torrent")
}

fn compute_only_files<ByteBuf: AsRef<[u8]>>(
    torrent: &TorrentMetaV1Info<ByteBuf>,
    filename_re: &str,
) -> anyhow::Result<Vec<usize>> {
    let filename_re = regex::Regex::new(filename_re).context("filename regex is incorrect")?;
    let mut only_files = Vec::new();
    for (idx, (filename, _)) in torrent.iter_filenames_and_lengths()?.enumerate() {
        let full_path = filename
            .to_pathbuf()
            .with_context(|| format!("filename of file {idx} is not valid utf8"))?;
        if filename_re.is_match(full_path.to_str().unwrap()) {
            only_files.push(idx);
        }
    }
    if only_files.is_empty() {
        anyhow::bail!("none of the filenames match the given regex")
    }
    Ok(only_files)
}

#[derive(Default, Clone)]
pub struct AddTorrentOptions {
    pub only_files_regex: Option<String>,
    pub overwrite: bool,
    pub list_only: bool,
    pub output_folder: Option<String>,
    pub sub_folder: Option<String>,
    pub peer_opts: Option<PeerConnectionOptions>,
    pub force_tracker_interval: Option<Duration>,
    pub paused: bool,
}

pub struct ListOnlyResponse {
    pub info_hash: Id20,
    pub info: TorrentMetaV1Info<ByteString>,
    pub only_files: Option<Vec<usize>>,
}

pub enum AddTorrentResponse {
    AlreadyManaged(ManagedTorrent),
    ListOnly(ListOnlyResponse),
    Added(TorrentManagerHandle),
}

#[derive(Default)]
pub struct SessionOptions {
    pub disable_dht: bool,
    pub disable_dht_persistence: bool,
    pub dht_config: Option<PersistentDhtConfig>,
    pub peer_id: Option<Id20>,
    pub peer_opts: Option<PeerConnectionOptions>,
}

#[derive(Clone, Debug)]
pub enum TorrentMeta {
    Info {
        info_hash: Id20,
        info: TorrentMetaV1Info<ByteString>,
        trackers: Vec<String>,
    },
    Full(TorrentMetaV1<ByteString>),
}

impl TorrentMeta {
    pub fn magnet_link(&self) -> String {
        Magnet {
            info_hash: self.info_hash(),
            trackers: self.trackers(),
        }
        .to_link()
    }

    pub fn info_hash(&self) -> Id20 {
        match self {
            TorrentMeta::Info { info_hash, .. } => info_hash.to_owned(),
            TorrentMeta::Full(t) => t.info_hash,
        }
    }

    pub fn info(&self) -> TorrentMetaV1Info<ByteString> {
        match self {
            TorrentMeta::Info { info, .. } => info.to_owned(),
            TorrentMeta::Full(t) => t.info.to_owned(),
        }
    }

    pub fn trackers(&self) -> Vec<String> {
        match self {
            TorrentMeta::Info { trackers, .. } => trackers.to_vec(),
            TorrentMeta::Full(t) => t
                .iter_announce()
                .filter_map(|tracker| {
                    let url = match std::str::from_utf8(tracker.as_ref()) {
                        Ok(url) => url,
                        Err(_) => {
                            warn!("cannot parse tracker url as utf-8, ignoring");
                            return None;
                        }
                    };
                    Some(url.to_string())
                })
                .collect::<Vec<_>>(),
        }
    }
}

impl Session {
    pub async fn new(output_folder: PathBuf, spawner: BlockingSpawner) -> anyhow::Result<Self> {
        Self::new_with_opts(output_folder, spawner, SessionOptions::default()).await
    }
    pub async fn new_with_opts(
        output_folder: PathBuf,
        spawner: BlockingSpawner,
        opts: SessionOptions,
    ) -> anyhow::Result<Self> {
        let peer_id = opts.peer_id.unwrap_or_else(generate_peer_id);
        let dht = if opts.disable_dht {
            None
        } else {
            let dht = if opts.disable_dht_persistence {
                Dht::new().await
            } else {
                PersistentDht::create(opts.dht_config).await
            }
            .context("error initializing DHT")?;
            Some(dht)
        };
        let peer_opts = opts.peer_opts.unwrap_or_default();

        Ok(Self {
            peer_id,
            dht,
            peer_opts,
            spawner,
            output_folder,
            locked: RwLock::new(SessionLocked::default()),
        })
    }
    pub fn get_dht(&self) -> Option<Dht> {
        self.dht.clone()
    }
    pub fn with_torrents<F>(&self, callback: F)
    where
        F: Fn(&[ManagedTorrent]),
    {
        callback(&self.locked.read().torrents)
    }

    pub async fn parse_torrent(&self, url: &str) -> anyhow::Result<TorrentMeta> {
        if url.starts_with("magnet:") {
            Ok(self.torrent_from_magnet(url).await.unwrap())
        } else if url.starts_with("http://") || url.starts_with("https://") {
            Ok(TorrentMeta::Full(torrent_from_url(url).await.unwrap()))
        } else {
            Ok(TorrentMeta::Full(torrent_from_file(url).unwrap()))
        }
    }

    pub async fn add_torrent(
        &self,
        torrent: TorrentMeta,
        opts: &AddTorrentOptions,
    ) -> anyhow::Result<AddTorrentResponse> {
        let dht_rx = match self.dht.as_ref() {
            Some(dht) => Some(dht.get_peers(torrent.info_hash()).await?),
            None => None,
        };
        self.main_torrent_info(
            torrent.info_hash(),
            torrent.info(),
            dht_rx,
            Vec::new(),
            torrent.trackers(),
            opts.clone(),
        )
        .await
    }

    async fn torrent_from_magnet(&self, url: &str) -> anyhow::Result<TorrentMeta> {
        let Magnet {
            info_hash,
            trackers,
        } = Magnet::parse(url).context("provided path is not a valid magnet URL")?;

        let dht_rx = self
            .dht
            .as_ref()
            .context("magnet links without DHT are not supported")?
            .get_peers(info_hash)
            .await?;

        let info = match read_metainfo_from_peer_receiver(
            self.peer_id,
            info_hash,
            dht_rx,
            Some(self.peer_opts),
            Arc::new(AtomicBool::new(false)), // this is a oneof
        )
        .await
        {
            ReadMetainfoResult::Found { info, .. } => info,
            ReadMetainfoResult::ChannelClosed { .. } => {
                anyhow::bail!("DHT died, no way to discover torrent metainfo")
            }
        };
        Ok(TorrentMeta::Info {
            info_hash,
            info,
            trackers,
        })
    }

    pub async fn torrent_from_info_hash(
        &self,
        info_hash: &str,
    ) -> anyhow::Result<TorrentMetaV1Info<ByteString>> {
        let info_hash = Id20::from_str(info_hash).unwrap();
        let dht_rx = self
            .dht
            .as_ref()
            .context("magnet links without DHT are not supported")?
            .get_peers(info_hash)
            .await?;

        let info = match read_metainfo_from_peer_receiver(
            self.peer_id,
            info_hash,
            dht_rx,
            Some(self.peer_opts),
            Arc::new(AtomicBool::new(false)), // this is a oneof
        )
        .await
        {
            ReadMetainfoResult::Found { info, .. } => info,
            ReadMetainfoResult::ChannelClosed { .. } => {
                anyhow::bail!("DHT died, no way to discover torrent metainfo")
            }
        };
        Ok(info)
    }

    #[allow(clippy::too_many_arguments)]
    async fn main_torrent_info(
        &self,
        info_hash: Id20,
        info: TorrentMetaV1Info<ByteString>,
        dht_peer_rx: Option<impl StreamExt<Item = SocketAddr> + Unpin + Send + Sync + 'static>,
        initial_peers: Vec<SocketAddr>,
        trackers: Vec<String>,
        opts: AddTorrentOptions,
    ) -> anyhow::Result<AddTorrentResponse> {
        debug!("Torrent info: {:#?}", &info);
        let only_files = if let Some(filename_re) = opts.only_files_regex {
            let only_files = compute_only_files(&info, &filename_re)?;
            for (idx, (filename, _)) in info.iter_filenames_and_lengths()?.enumerate() {
                if !only_files.contains(&idx) {
                    continue;
                }
                if !opts.list_only {
                    info!("Will download {:?}", filename);
                }
            }
            Some(only_files)
        } else {
            None
        };

        if opts.list_only {
            return Ok(AddTorrentResponse::ListOnly(ListOnlyResponse {
                info_hash,
                info,
                only_files,
            }));
        }

        let sub_folder = opts.sub_folder.map(PathBuf::from).unwrap_or_default();
        let output_folder = opts
            .output_folder
            .map(PathBuf::from)
            .unwrap_or_else(|| self.output_folder.clone())
            .join(sub_folder);

        let managed_torrent = ManagedTorrent {
            info_hash,
            output_folder: output_folder.clone(),
            state: ManagedTorrentState::Initializing,
        };

        match self.locked.write().add_torrent(managed_torrent) {
            SessionLockedAddTorrentResult::AlreadyManaged(managed) => {
                return Ok(AddTorrentResponse::AlreadyManaged(managed))
            }
            SessionLockedAddTorrentResult::Added(_) => {}
        }

        let mut builder = TorrentManagerBuilder::new(info, info_hash, output_folder.clone());
        builder
            .overwrite(opts.overwrite)
            .spawner(self.spawner)
            .peer_id(self.peer_id);
        if let Some(only_files) = only_files {
            builder.only_files(only_files);
        }
        if let Some(interval) = opts.force_tracker_interval {
            builder.force_tracker_interval(interval);
        }

        if let Some(t) = opts.peer_opts.unwrap_or(self.peer_opts).connect_timeout {
            builder.peer_connect_timeout(t);
        }

        if let Some(t) = opts.peer_opts.unwrap_or(self.peer_opts).read_write_timeout {
            builder.peer_read_write_timeout(t);
        }

        let handle = match builder
            .start_manager()
            .context("error starting torrent manager")
        {
            Ok(handle) => {
                let mut g = self.locked.write();
                let m = g
                    .torrents
                    .iter_mut()
                    .find(|t| t.info_hash == info_hash && t.output_folder == output_folder)
                    .unwrap();
                m.state = ManagedTorrentState::Running(handle.clone());
                handle
            }
            Err(error) => {
                let mut g = self.locked.write();
                let idx = g
                    .torrents
                    .iter()
                    .position(|t| t.info_hash == info_hash && t.output_folder == output_folder)
                    .unwrap();
                g.torrents.remove(idx);
                return Err(error);
            }
        };
        {
            let mut g = self.locked.write();
            let m = g
                .torrents
                .iter_mut()
                .find(|t| t.info_hash == info_hash && t.output_folder == output_folder)
                .unwrap();
            m.state = ManagedTorrentState::Running(handle.clone());
        }

        for url in trackers {
            match reqwest::Url::parse(&url) {
                Ok(url) => {
                    handle.add_tracker(url);
                }
                Err(e) => {
                    warn!("error parsing tracker {} as url: {}", url, e);
                }
            }
        }
        for peer in initial_peers {
            handle.add_peer(peer);
        }

        if let Some(mut dht_peer_rx) = dht_peer_rx {
            spawn("DHT peer adder", {
                let handle = handle.clone();
                async move {
                    while let Some(peer) = dht_peer_rx.next().await {
                        if handle.canceled() { // TODO: some sort of select ?
                            return Ok(());
                        }
                        handle.add_peer(peer);
                    }
                    warn!("dht was closed");
                    Ok(())
                }
            });
        }

        Ok(AddTorrentResponse::Added(handle))
    }

    pub fn stop_torrent(&self, info_hash_str: &str) -> StopTorrentResult {
        let info_hash = Id20::from_str(info_hash_str).expect("invalid info_hash");
        let mut g = self.locked.write();
        g.stop_torrent(info_hash)
    }
}
