fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();

    twprs::tsg::extract(&args[1], std::io::stdout())?;

    Ok(())
}
