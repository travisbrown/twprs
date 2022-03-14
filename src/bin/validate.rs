fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let file = std::fs::File::open(&args[1])?;
    let reader = twprs::avro::reader(file)?;

    let count = twprs::avro::validate(reader)?;
    println!("Valid file with {} records", count);

    Ok(())
}
