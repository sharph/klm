mod k8s;
mod tui;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tui = tui::Tui::new().await?;

    tui.enter()?;

    loop {
        tui.draw()?;
        let message = tui.next().await;
        if matches!(message, tui::TuiMessage::Exit) {
            break;
        }
        tui.handle_message(message)
            .await
            .expect("unhandled message type");
    }
    tui.exit()?;
    Ok(())
}
