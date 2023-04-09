use clap::{ Args, Parser, Subcommand };

#[derive(Debug, Parser)]
#[clap(author, version, about)]
pub struct AppArgs{
    /*/// Output to files based on topic names rather than stdout.
    pub f: bool,*/
    /// The first argument
    pub first_arg: String,
    /// The second argument
    pub second_arg: String,
    /// The third argument
    pub third_arg: String,
}


/*
- File output flag (topic as filename)
- Properties file name override
- Message key override
- Topics to subscribe to (comma seperated)
- Group ID to use
- Bootstrap server override
- Autocommit flag override
*/