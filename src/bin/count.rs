fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let file = std::fs::File::open(&args[1])?;
    let reader = twprs::avro::reader(file)?;
    let mut counts = twprs::avro::count_users(reader)?
        .into_iter()
        .collect::<Vec<_>>();
    counts.sort_by_key(|(_, count)| std::cmp::Reverse(*count));

    for ((user_id, screen_name), count) in counts {
        println!("{},{},{}", user_id, screen_name, count);
    }

    Ok(())
}
