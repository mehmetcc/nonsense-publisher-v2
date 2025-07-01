use chrono::{DateTime, Utc};
use fake::faker::address::raw::{CityName, CountryName, StreetName, ZipCode};
use fake::faker::company::raw::CompanyName;
use fake::faker::name::raw::{FirstName, LastName, Title};
use fake::locales::EN;
use fake::Fake;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Nonsense {
    pub id: Uuid,
    pub data: Person,
    pub created_at: DateTime<Utc>,
}

impl Nonsense {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            data: Person::new(),
            created_at: Utc::now(),
        }
    }

    pub fn to_json(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).map_err(anyhow::Error::from)
    }
}

#[derive(Debug, Serialize)]
pub struct Person {
    pub id: Uuid,
    pub title: String,
    pub first_name: String,
    pub last_name: String,
    pub city_name: String,
    pub company_name: String,
    pub street_name: String,
    pub zipcode: String,
    pub country: String,
}

impl Person {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            title: Title(EN).fake(),
            first_name: FirstName(EN).fake(),
            last_name: LastName(EN).fake(),
            city_name: CityName(EN).fake(),
            company_name: CompanyName(EN).fake(),
            street_name: StreetName(EN).fake(),
            zipcode: ZipCode(EN).fake(),
            country: CountryName(EN).fake(),
        }
    }
}
