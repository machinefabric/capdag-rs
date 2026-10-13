use capdag::FabricRegistry;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing specific problematic cap...");

    let registry = FabricRegistry::new().await?;

    // Test the exact cap that was failing
    let problematic_urn = "cap:bitlogic;language=en;constrained";
    println!("Fetching: {}", problematic_urn);

    match registry.get_cap(problematic_urn).await {
        Ok(cap) => {
            println!("SUCCESS: Cap parsed correctly!");
            println!("  URN: {}", cap.urn_string());
            println!("  Command: {}", cap.primary_alias());
            println!(
                "  Description: {}",
                cap.cap_description.as_ref().unwrap_or(&"None".to_string())
            );
            println!("  Accepts stdin: {}", cap.accepts_stdin());
            if let Some(stdin_urn) = cap.get_stdin_media_urn() {
                println!("  Stdin media URN: {}", stdin_urn);
            }

            let args = cap.get_args();
            let required_args: Vec<_> = args.iter().filter(|a| a.required).collect();
            let optional_args: Vec<_> = args.iter().filter(|a| !a.required).collect();

            if !required_args.is_empty() {
                println!("  Required args: {}", required_args.len());
                for arg in required_args {
                    println!("    - {}", arg.media_urn);
                }
            }

            if !optional_args.is_empty() {
                println!("  Optional args: {}", optional_args.len());
                for arg in optional_args {
                    println!("    - {}", arg.media_urn);
                }
            }

            if let Some(output) = cap.main_output() {
                println!(
                    "  Output: {} - {}",
                    output.media_urn, output.output_description
                );
            }
        }
        Err(e) => {
            println!("FAILED: {}", e);
            return Err(e.into());
        }
    }

    println!("\nTest completed successfully!");
    Ok(())
}
