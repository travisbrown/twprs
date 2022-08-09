use octocrab::{
    models::IssueState,
    params::{issues::Sort, State},
    Octocrab,
};

pub struct Repo {
    client: Octocrab,
    owner: String,
    repo: String,
}

impl Repo {
    /// Initialize a repo / client instance with defaults and configuration
    pub fn new(owner: &str, repo: &str, token: Option<String>) -> Result<Self, Error> {
        let builder = octocrab::OctocrabBuilder::new();

        let client = match token {
            Some(value) => builder.personal_token(value).build(),
            None => builder.build(),
        }?;

        Ok(Self {
            client,
            owner: owner.to_string(),
            repo: repo.to_string(),
        })
    }

    pub async fn next_issue(&self) -> Result<Option<(u64, Option<String>)>, Error> {
        let issues = self
            .client
            .issues(&self.owner, &self.repo)
            .list()
            .state(State::Open)
            .labels(&vec!["approved".to_string()])
            .sort(Sort::Created)
            .per_page(1)
            .send()
            .await?;

        let re = regex::Regex::new(r"follower report for (\w{3,16})\W").unwrap();

        match issues.items.into_iter().next() {
            Some(issue) => {
                let screen_name = match re.captures(&issue.title) {
                    Some(captures) => Some(captures[1].to_string()),
                    None => issue.body.and_then(|text| {
                        re.captures(&text).map(|captures| captures[1].to_string())
                    }),
                };

                Ok(Some((issue.number as u64, screen_name)))
            }
            None => Ok(None),
        }
    }

    pub async fn close_issue(&self, number: u64) -> Result<(), Error> {
        self.client
            .issues(&self.owner, &self.repo)
            .create_comment(
                number,
                "I don't understand this request, sorry! (This is an automated response.)",
            )
            .await?;
        self.client
            .issues(&self.owner, &self.repo)
            .remove_label(number, "approved")
            .await?;
        self.client
            .issues(&self.owner, &self.repo)
            .update(number)
            .state(IssueState::Closed)
            .send()
            .await?;

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("GitHub client error")]
    Octocrab(#[from] octocrab::Error),
}
