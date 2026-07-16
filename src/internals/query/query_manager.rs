use crate::internals::{
    context::context_manager::Track, parsing::deserialize, search::search_manager::SearchItem,
};
use anyhow::{Context, bail};
use spotify_rs::model::PlayableItem;
use spotify_rs::model::track::SimplifiedTrack;

/// Which kind of Spotify resource a sync targets. Playlists, albums, and single tracks all
/// resolve to a list of tracks that the workers then search + download; only the initial Spotify
/// fetch differs, so the rest of the pipeline is identical across kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ResourceKind {
    #[default]
    Playlist,
    Album,
    Track,
}

impl ResourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ResourceKind::Playlist => "playlist",
            ResourceKind::Album => "album",
            ResourceKind::Track => "track",
        }
    }

    /// Parse a resource kind from the string the API/frontend sends. Unknown or empty values fall
    /// back to `Playlist` so existing (playlist-only) callers and persisted runs keep working.
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "album" => ResourceKind::Album,
            "track" => ResourceKind::Track,
            _ => ResourceKind::Playlist,
        }
    }
}

impl std::fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

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

    /// Resolve whichever Spotify resource `kind` names into `(display_name, tracks)`. The name is
    /// used to file the downloads into a per-resource folder; the tracks feed the search/download
    /// pipeline exactly as playlist tracks always have.
    pub async fn fetch_with_name(self, kind: ResourceKind) -> anyhow::Result<(String, Vec<Track>)> {
        match kind {
            ResourceKind::Playlist => self.fetch_playlist_with_name().await,
            ResourceKind::Album => self.fetch_album_with_name().await,
            ResourceKind::Track => self.fetch_track_with_name().await,
        }
    }

    /// Fetch every track on a Spotify album, returning the album name alongside them so callers can
    /// organise the downloaded files into a per-album folder.
    pub async fn fetch_album_with_name(self) -> anyhow::Result<(String, Vec<Track>)> {
        let album_id = parse_spotify_album_id(&self.playlist_url)?;
        let (client_id, client_secret) = self.spotify_client_credentials()?;
        let spotify = spotify_rs::ClientCredsClient::authenticate(client_id, client_secret)
            .await
            .context("Authenticate Spotify client credentials")?;

        let album = spotify_rs::album(album_id)
            .market("US")
            .get(&spotify)
            .await
            .context("Fetch Spotify album")?;
        let name = album.name.clone();
        let tracks = album
            .tracks
            .get_all(&spotify)
            .await
            .context("Paginating album tracks")?
            .into_iter()
            .flatten()
            .flat_map(|track| {
                simplified_track_to_search_item(track, &name)
                    .map(Track::Query)
                    .inspect_err(|err| tracing::warn!(?err, "Skipping Spotify album item"))
                    .ok()
            })
            .collect::<Vec<_>>();
        tracing::info!(album = %name, track_count = tracks.len(), "Fetched Spotify album");
        Ok((name, tracks))
    }

    /// Fetch a single Spotify track. Returns the track's album name so the file is filed under the
    /// same per-album folder an album sync would use.
    pub async fn fetch_track_with_name(self) -> anyhow::Result<(String, Vec<Track>)> {
        let track = self.fetch_track().await?;
        let name = match &track {
            Track::Query(item) => item.album.clone(),
            _ => String::new(),
        };
        Ok((name, vec![track]))
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

/// Album-track responses carry only a simplified track (no embedded album), so the album name is
/// supplied by the caller from the parent album.
fn simplified_track_to_search_item(
    song: SimplifiedTrack,
    album_name: &str,
) -> anyhow::Result<SearchItem> {
    let artist = song
        .artists
        .first()
        .with_context(|| format!("Spotify track {} has no artist", song.name))?;
    Ok(SearchItem::new(
        song.id,
        song.name,
        album_name.to_string(),
        artist.name.clone(),
    ))
}

pub fn parse_spotify_track_id(input: &str) -> anyhow::Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("Spotify track URL is empty");
    }
    parse_spotify_resource_id(trimmed, "track")
}

pub fn parse_spotify_album_id(input: &str) -> anyhow::Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("Spotify album URL is empty");
    }
    parse_spotify_resource_id(trimmed, "album")
}

/// Extract a Spotify resource id of the given `kind` ("track"/"album"/…) from an
/// `open.spotify.com/<kind>/<id>` URL, a `spotify:<kind>:<id>` URI, or a bare 22-char id.
fn parse_spotify_resource_id(trimmed: &str, kind: &str) -> anyhow::Result<String> {
    let uri_prefix = format!("spotify:{kind}:");
    if let Some(rest) = trimmed.strip_prefix(&uri_prefix) {
        let id = rest.split('?').next().unwrap_or(rest);
        return validate_resource_id(id, kind);
    }
    let path_segment = format!("/{kind}/");
    if let Some(index) = trimmed.find(&path_segment) {
        let rest = &trimmed[index + path_segment.len()..];
        let id = rest.split(['?', '/', '#']).next().unwrap_or(rest);
        return validate_resource_id(id, kind);
    }
    validate_resource_id(trimmed, kind)
}

fn validate_resource_id(id: &str, kind: &str) -> anyhow::Result<String> {
    if id.len() == 22 && id.chars().all(|c| c.is_ascii_alphanumeric()) {
        Ok(id.to_string())
    } else {
        bail!("Expected a Spotify {kind} URL, URI, or 22-character {kind} ID")
    }
}

#[cfg(test)]
mod tests {
    use super::{ResourceKind, parse_spotify_album_id, parse_spotify_track_id};

    #[test]
    fn parses_open_spotify_album_url() {
        assert_eq!(
            parse_spotify_album_id("https://open.spotify.com/album/1DdpxHPTsrivn3X0KumOQz?si=x")
                .unwrap(),
            "1DdpxHPTsrivn3X0KumOQz"
        );
    }

    #[test]
    fn parses_spotify_album_uri() {
        assert_eq!(
            parse_spotify_album_id("spotify:album:1DdpxHPTsrivn3X0KumOQz").unwrap(),
            "1DdpxHPTsrivn3X0KumOQz"
        );
    }

    #[test]
    fn rejects_track_url_for_album_mode() {
        assert!(parse_spotify_album_id("https://open.spotify.com/track/abc").is_err());
    }

    #[test]
    fn resource_kind_parses_known_and_unknown_values() {
        assert_eq!(ResourceKind::parse("album"), ResourceKind::Album);
        assert_eq!(ResourceKind::parse("TRACK"), ResourceKind::Track);
        assert_eq!(ResourceKind::parse("playlist"), ResourceKind::Playlist);
        // Unknown / empty falls back to playlist for backward compatibility.
        assert_eq!(ResourceKind::parse(""), ResourceKind::Playlist);
        assert_eq!(ResourceKind::parse("bogus"), ResourceKind::Playlist);
    }

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
