use crate::internals::{
    context::context_manager::Track, parsing::deserialize, search::search_manager::SearchItem,
};
use anyhow::Context;
use spotify_rs::model::PlayableItem;

#[derive(Debug, Clone)]
pub struct QueryManager {
    pub playlist_url: String,
    client_id: String,
    client_secret: String,
}

impl QueryManager {
    pub fn new(
        playlist_url: impl Into<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
    ) -> Self {
        let playlist_url = playlist_url.into();
        let client_id = client_id.unwrap();
        let client_secret = client_secret.unwrap();
        QueryManager {
            playlist_url,
            client_id,
            client_secret,
        }
    }
    pub async fn fetch_playlist(self) -> anyhow::Result<Vec<Track>> {
        let spotify =
            spotify_rs::ClientCredsClient::authenticate(self.client_id, self.client_secret)
                .await
                .unwrap();

        let playlist = spotify_rs::playlist(self.playlist_url)
            .market("US")
            .get(&spotify)
            .await
            .unwrap();
        let pl = playlist
            .tracks
            .get_all(&spotify)
            .await
            .context("Paginating")?
            .into_iter()
            .flatten()
            .flat_map(|track| {
                if let PlayableItem::Track(song) = track.track {
                    let song2 = song.clone();
                    Some(Track::Query(SearchItem::new(
                        song.clone().name,
                        song.clone().album.name,
                        song2.artists.clone().first().unwrap().name.clone(),
                    )))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        println!("Playlist has {} songs", pl.len());
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
