use crate::internals::{
    context::context_manager::Track, parsing::deserialize, search::search_manager::SearchItem,
};
use anyhow::{Context, bail};
use spotify_rs::model::PlayableItem;

#[derive(Debug, Clone)]
pub struct QueryManager {
    pub playlist_url: String,
    pub search_timeout_secs: u8,
    client_id: Option<String>,
    client_secret: Option<String>,
}

impl QueryManager {
    pub fn new(
        playlist_url: impl Into<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
    ) -> Self {
        Self::new_with_timeout(playlist_url, client_id, client_secret, 10)
    }

    pub fn new_with_timeout(
        playlist_url: impl Into<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        search_timeout_secs: u8,
    ) -> Self {
        let playlist_url = playlist_url.into();
        QueryManager {
            playlist_url,
            search_timeout_secs,
            client_id,
            client_secret,
        }
    }
    pub async fn fetch_playlist(self) -> anyhow::Result<Vec<Track>> {
        let Some(client_id) = self.client_id else {
            bail!("CLIENT_ID is required to fetch Spotify playlist");
        };
        let Some(client_secret) = self.client_secret else {
            bail!("CLIENT_SECRET is required to fetch Spotify playlist");
        };
        let spotify = spotify_rs::ClientCredsClient::authenticate(client_id, client_secret)
            .await
            .context("Authenticate Spotify client credentials")?;

        let playlist = spotify_rs::playlist(self.playlist_url)
            .market("US")
            .get(&spotify)
            .await
            .context("Fetch Spotify playlist")?;
        let pl = playlist
            .tracks
            .get_all(&spotify)
            .await
            .context("Paginating")?
            .into_iter()
            .flatten()
            .flat_map(|track| {
                if let PlayableItem::Track(song) = track.track {
                    let Some(artist) = song.artists.first() else {
                        tracing::warn!(track = song.name, "Skipping Spotify track without artist");
                        return None;
                    };
                    Some(Track::Query(SearchItem::new(
                        song.id,
                        song.name,
                        song.album.name,
                        artist.name.clone(),
                    )))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        tracing::info!(track_count = pl.len(), "Fetched Spotify playlist");
        Ok(pl)
    }
    pub async fn run(&self) -> anyhow::Result<Vec<Track>> {
        let data_string = include_str!("../parsing/sample.json");
        let data: deserialize::Playlist =
            serde_json::from_str(data_string).context("Deserializing")?;
        let queries: Vec<SearchItem> = data.into();
        let vals = queries.into_iter().map(Track::Query).collect();
        Ok(vals)
    }
}
