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
    fn spotify_client_credentials(&self) -> anyhow::Result<(String, String)> {
        let Some(client_id) = self.client_id.clone() else {
            bail!("CLIENT_ID is required to fetch Spotify metadata");
        };
        let Some(client_secret) = self.client_secret.clone() else {
            bail!("CLIENT_SECRET is required to fetch Spotify metadata");
        };
        Ok((client_id, client_secret))
    }

    pub async fn fetch_playlist(self) -> anyhow::Result<Vec<Track>> {
        Ok(self.fetch_playlist_with_name().await?.1)
    }

    /// Like [`Self::fetch_playlist`], but also returns the playlist's Spotify name so callers
    /// can organise the downloaded files into a per-playlist folder.
    pub async fn fetch_playlist_with_name(self) -> anyhow::Result<(String, Vec<Track>)> {
        let (client_id, client_secret) = self.spotify_client_credentials()?;
        let spotify = spotify_rs::ClientCredsClient::authenticate(client_id, client_secret)
            .await
            .context("Authenticate Spotify client credentials")?;

        let playlist = spotify_rs::playlist(self.playlist_url)
            .market("US")
            .get(&spotify)
            .await
            .context("Fetch Spotify playlist")?;
        let name = playlist.name.clone();
        let pl = playlist
            .tracks
            .get_all(&spotify)
            .await
            .context("Paginating")?
            .into_iter()
            .flatten()
            .flat_map(|track| {
                if let PlayableItem::Track(song) = track.track {
                    spotify_track_to_search_item(song)
                        .map(Track::Query)
                        .inspect_err(|err| tracing::warn!(?err, "Skipping Spotify playlist item"))
                        .ok()
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        tracing::info!(playlist = %name, track_count = pl.len(), "Fetched Spotify playlist");
        Ok((name, pl))
    }

    pub async fn fetch_track(self) -> anyhow::Result<Track> {
        let track_id = parse_spotify_track_id(&self.playlist_url)?;
        let (client_id, client_secret) = self.spotify_client_credentials()?;
        let spotify = spotify_rs::ClientCredsClient::authenticate(client_id, client_secret)
            .await
            .context("Authenticate Spotify client credentials")?;
        let song = spotify_rs::track(track_id)
            .market("US")
            .get(&spotify)
            .await
            .context("Fetch Spotify track")?;
        let item = spotify_track_to_search_item(song).context("Build search item from track")?;
        tracing::info!(track_id = %item.track_id, track = %item.track, artist = %item.artist, "Fetched Spotify track");
        Ok(Track::Query(item))
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

fn spotify_track_to_search_item(
    song: spotify_rs::model::track::Track,
) -> anyhow::Result<SearchItem> {
    let artist = song
        .artists
        .first()
        .with_context(|| format!("Spotify track {} has no artist", song.name))?;
    Ok(SearchItem::new(
        song.id,
        song.name,
        song.album.name,
        artist.name.clone(),
    ))
}

pub fn parse_spotify_track_id(input: &str) -> anyhow::Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("Spotify track URL is empty");
    }
    if let Some(rest) = trimmed.strip_prefix("spotify:track:") {
        let id = rest.split('?').next().unwrap_or(rest);
        return validate_track_id(id);
    }
    if let Some(index) = trimmed.find("/track/") {
        let rest = &trimmed[index + "/track/".len()..];
        let id = rest.split(['?', '/', '#']).next().unwrap_or(rest);
        return validate_track_id(id);
    }
    validate_track_id(trimmed)
}

fn validate_track_id(id: &str) -> anyhow::Result<String> {
    if id.len() == 22 && id.chars().all(|c| c.is_ascii_alphanumeric()) {
        Ok(id.to_string())
    } else {
        bail!("Expected a Spotify track URL, URI, or 22-character track ID")
    }
}

#[cfg(test)]
mod tests {
    use super::parse_spotify_track_id;

    #[test]
    fn parses_open_spotify_track_url() {
        assert_eq!(
            parse_spotify_track_id("https://open.spotify.com/track/1DdpxHPTsrivn3X0KumOQz?si=test")
                .unwrap(),
            "1DdpxHPTsrivn3X0KumOQz"
        );
    }

    #[test]
    fn parses_spotify_track_uri() {
        assert_eq!(
            parse_spotify_track_id("spotify:track:1DdpxHPTsrivn3X0KumOQz").unwrap(),
            "1DdpxHPTsrivn3X0KumOQz"
        );
    }

    #[test]
    fn rejects_playlist_url_for_track_mode() {
        assert!(parse_spotify_track_id("https://open.spotify.com/playlist/abc").is_err());
    }
}
