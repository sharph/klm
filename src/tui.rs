use std::future::IntoFuture;

use crate::k8s::{LogIdentifier, LogLine, LogStreamManager, LogStreamManagerMessage};
use crossterm::event::KeyCode;
use futures::StreamExt;
use ratatui::backend::CrosstermBackend as Backend;
use ratatui::crossterm::{
    cursor,
    event::{Event as CrosstermEvent, EventStream, KeyEvent},
    terminal::{EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::prelude::*;
use ratatui::widgets::{Row, Table, TableState};
use std::error::Error;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Error, Debug)]
enum TuiError {
    #[error("unhandlable message")]
    UnhandleableMessage,
}

pub enum TuiMessage {
    K8SMessage(LogStreamManagerMessage),
    Key(KeyEvent),
    Exit,
}

pub struct Tui {
    pub terminal: ratatui::Terminal<Backend<std::io::Stderr>>,
    pub task: JoinHandle<()>,
    pub cancellation_token: CancellationToken,
    pub tx: tokio::sync::mpsc::Sender<TuiMessage>,
    pub rx: tokio::sync::mpsc::Receiver<TuiMessage>,
    messages: Vec<LogLine>,
    log_table_state: TableState,
    source_table_state: TableState,
    log_stream_manager: LogStreamManager,
    sources: Vec<LogIdentifier>,
}

impl Tui {
    pub async fn new() -> Result<Self, Box<dyn Error>> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let (ls_tx, mut ls_rx) = tokio::sync::mpsc::channel(32);
        let tx2 = tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = ls_rx.recv().await {
                let _ = tx2.send(TuiMessage::K8SMessage(msg)).await;
            }
        });
        let log_stream_manager = LogStreamManager::new(ls_tx)
            .await
            .expect("log stream manager failed to setup");
        Ok(Self {
            terminal: ratatui::Terminal::new(Backend::new(std::io::stderr()))?,
            task: tokio::spawn(async {}),
            cancellation_token: CancellationToken::new(),
            tx,
            rx,
            messages: vec![],
            log_table_state: TableState::new(),
            source_table_state: TableState::new(),
            log_stream_manager,
            sources: vec![],
        })
    }

    pub fn start(&mut self) {
        let tx = self.tx.clone();
        let mut reader = EventStream::new();
        tokio::spawn(async move {
            loop {
                let event = reader.next().into_future().await;
                match event {
                    Some(Ok(CrosstermEvent::Key(key))) => {
                        tx.send(TuiMessage::Key(key))
                            .await
                            .expect("couldn't send keypress");
                    }
                    _ => {}
                }
            }
        });
    }

    pub fn stop(&self) {}

    pub fn draw(&mut self) -> Result<(), Box<dyn Error>> {
        self.terminal
            .draw(|frame: &mut Frame| {
                let layout = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints(vec![Constraint::Percentage(75), Constraint::Fill(1)])
                    .split(frame.area());
                // log messages
                let rows = self
                    .messages
                    .iter()
                    .map(|log| Row::new(vec![log.id.pod.clone(), log.message.clone()]));
                let table = Table::new(rows, [Constraint::Min(10), Constraint::Percentage(75)]);
                self.log_table_state.select_last();
                frame.render_stateful_widget(table, layout[0], &mut self.log_table_state);
                // log sources
                let rows = self.sources.iter().map(|s| {
                    let mut row = Row::new(vec![s.pod.clone()]);
                    if self.log_stream_manager.streams.get(&s).is_some() {
                        row = row.bold();
                    } else {
                        row = row.dim();
                    }
                    row
                });
                let table = Table::new(rows, [Constraint::Fill(1)])
                    .highlight_style(Style::new().reversed());
                frame.render_stateful_widget(table, layout[1], &mut self.source_table_state);
            })
            .expect("drawing failed");
        Ok(())
    }

    pub fn enter(&mut self) -> Result<(), Box<dyn Error>> {
        crossterm::terminal::enable_raw_mode()?;
        crossterm::execute!(std::io::stderr(), EnterAlternateScreen, cursor::Hide)?;
        self.start();
        Ok(())
    }

    pub fn exit(&mut self) -> Result<(), Box<dyn Error>> {
        self.stop();
        if crossterm::terminal::is_raw_mode_enabled()? {
            crossterm::execute!(std::io::stderr(), LeaveAlternateScreen, cursor::Show)?;
        }
        Ok(())
    }

    pub async fn next(&mut self) -> TuiMessage {
        self.rx
            .recv()
            .await
            .expect("Receiver should not be cleaned up after sender")
    }

    fn key_up(&mut self) {
        self.source_table_state.select_previous()
    }

    fn key_down(&mut self) {
        self.source_table_state.select_next()
    }

    fn key_enter(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(idx) = self.source_table_state.selected() {
            let id = self.sources.get(idx).expect("index out of bounds");
            if self.log_stream_manager.streams.get(&id).is_none() {
                self.log_stream_manager.add_stream(id.clone());
            } else {
                self.log_stream_manager.drop_stream(&id)?;
            }
        }
        Ok(())
    }

    pub async fn handle_message(&mut self, message: TuiMessage) -> Result<(), Box<dyn Error>> {
        match message {
            TuiMessage::K8SMessage(msg) => match msg {
                LogStreamManagerMessage::Log(log) => self.messages.push(log),
                LogStreamManagerMessage::LogSourceCreated(src) => {
                    self.sources.push(src);
                }
                LogStreamManagerMessage::LogSourceRemoved(src) => {
                    self.sources.retain(|p| *p != src);
                }
            },
            TuiMessage::Key(key) => match key.code {
                KeyCode::Up => self.key_up(),
                KeyCode::Char('k') => self.key_up(),
                KeyCode::Down => self.key_down(),
                KeyCode::Char('j') => self.key_down(),
                KeyCode::Enter => self.key_enter()?,
                KeyCode::Esc => self.tx.send(TuiMessage::Exit).await?,
                _ => {}
            },
            _ => return Err(Box::new(TuiError::UnhandleableMessage)),
        }
        Ok(())
    }
}
