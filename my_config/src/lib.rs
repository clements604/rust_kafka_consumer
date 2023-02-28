use config::Config;
use serde_derive::Deserialize;

#[derive(Debug, Default, serde_derive::Deserialize, PartialEq, Eq)]
struct AppConfig {
    list: Vec<String>,
}

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}

pub fn load_cfg() {
    std::env::set_var("APP_LIST", "Hello World");

    let config = Config::builder()
        .add_source(
            config::Environment::with_prefix("APP")
                .try_parsing(true)
                .separator("_")
                .list_separator(" "),
        )
        .build()
        .unwrap();

    let app: AppConfig = config.try_deserialize().unwrap();

    println!("{:?}", app.list);

    assert_eq!(app.list, vec![String::from("Hello"), String::from("World")]);

    std::env::remove_var("APP_LIST");
}