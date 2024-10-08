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
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::sync::Arc;
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

#[derive(Default)]
struct SourceState {
    log_lines: BTreeSet<Arc<LogLine>>,
    closed: bool,
}

pub struct Tui {
    pub terminal: ratatui::Terminal<Backend<std::io::Stderr>>,
    pub task: JoinHandle<()>,
    pub cancellation_token: CancellationToken,
    pub tx: tokio::sync::mpsc::Sender<TuiMessage>,
    pub rx: tokio::sync::mpsc::Receiver<TuiMessage>,
    messages: BTreeSet<Arc<LogLine>>,
    sources: BTreeMap<LogIdentifier, SourceState>,
    log_table_state: TableState,
    source_table_state: TableState,
    log_stream_manager: LogStreamManager,
}

impl Tui {
    pub async fn new() -> Result<Self, Box<dyn Error>> {
        let (tx, rx) = tokio::sync::mpsc::channel(2); // a small buffer here keeps the sending
                                                      // fair and UI snappy
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
            messages: BTreeSet::new(),
            log_table_state: TableState::new(),
            source_table_state: TableState::new(),
            log_stream_manager,
            sources: BTreeMap::new(),
        })
    }

    pub fn start(&mut self) {
        let tx = self.tx.clone();
        let mut reader = EventStream::new();
        self.task = tokio::spawn(async move {
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

    pub fn stop(&mut self) {
        self.log_stream_manager
            .close()
            .expect("could not close stream manager");
        self.rx.close();
    }

    pub fn get_sparkline_for_source(&self, id: &LogIdentifier, buckets: usize) -> String {
        let mut buckets: Box<[u64]> = (0..buckets).map(|_| 0).collect();
        let period: i128 = 60 * 1_000_000_000;
        if let Some(last) = self.messages.last() {
            let lasttime = last.timestamp;
            let len = buckets.len();
            for (idx, val) in buckets.iter_mut().enumerate() {
                *val = self
                    .sources
                    .get(&id)
                    .map(|s| &s.log_lines)
                    .unwrap()
                    .iter()
                    .filter(|m| m.timestamp > lasttime - (len - idx) as i128 * period)
                    .filter(|m| m.timestamp <= lasttime - (len - idx - 1) as i128 * period)
                    .count() as u64;
            }
        }
        let max = buckets.iter().fold(1, |a, b| a.max(*b));
        buckets
            .iter()
            .map(|v| v * 7 / max) // always leave a little space at top of sparklines
            .map(|v| match v {
                0 => ratatui::symbols::bar::NINE_LEVELS.empty,
                1 => ratatui::symbols::bar::NINE_LEVELS.one_eighth,
                2 => ratatui::symbols::bar::NINE_LEVELS.one_quarter,
                3 => ratatui::symbols::bar::NINE_LEVELS.three_eighths,
                4 => ratatui::symbols::bar::NINE_LEVELS.half,
                5 => ratatui::symbols::bar::NINE_LEVELS.five_eighths,
                6 => ratatui::symbols::bar::NINE_LEVELS.three_quarters,
                7 => ratatui::symbols::bar::NINE_LEVELS.seven_eighths,
                8 => ratatui::symbols::bar::NINE_LEVELS.full,
                _ => "X",
            })
            .fold(String::new(), |a, b| a + b)
    }

    pub fn draw(&mut self) -> Result<(), Box<dyn Error>> {
        let mut sparkline_data: HashMap<LogIdentifier, String> = HashMap::new();
        for id in self.sources.keys() {
            sparkline_data.insert(id.clone(), self.get_sparkline_for_source(&id, 10));
        }
        self.terminal
            .draw(|frame: &mut Frame| {
                let layout = Layout::default()
                    .direction(if frame.area().right() > 200 {
                        Direction::Horizontal
                    } else {
                        Direction::Vertical
                    })
                    .constraints(vec![Constraint::Percentage(75), Constraint::Fill(1)])
                    .split(frame.area());
                // log messages
                let selected = self
                    .source_table_state
                    .selected()
                    .map(|i| self.sources.iter().nth(i))
                    .unwrap_or(None);
                let rows = self.messages.iter().map(|log| {
                    let mut row = Row::new(vec![log.id.format(), log.stripped.clone()]);
                    if Some(&log.id) == selected.map(|x| x.0) {
                        row = row.bold();
                    } else {
                        row = row.dim();
                    }
                    if self
                        .sources
                        .get(&log.id)
                        .expect("couldn't find log state")
                        .closed
                    {
                        row = row.red();
                    }
                    row
                });
                let table = Table::new(rows, [Constraint::Min(10), Constraint::Percentage(75)]);
                self.log_table_state = TableState::new();
                self.log_table_state.select_last();
                frame.render_stateful_widget(table, layout[0], &mut self.log_table_state);
                // log sources
                let rows = self.sources.iter().map(|s| {
                    let sparkline = sparkline_data
                        .get(&s.0)
                        .map(|v| v.clone())
                        .unwrap_or_else(|| "     ".to_string());
                    let mut row = Row::new(vec![sparkline, s.0.format()]);
                    if self.log_stream_manager.streams.get(&s.0).is_some() {
                        row = row.bold();
                        if self.sources.get(&s.0).map(|s| s.closed) == Some(true) {
                            row = row.red();
                        }
                    } else {
                        row = row.dim();
                    }
                    row
                });
                let table = Table::new(rows, [Constraint::Length(10), Constraint::Fill(1)])
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
            let id = self
                .sources
                .iter()
                .nth(idx)
                .map(|x| x.0)
                .expect("index out of bounds")
                .clone();
            if self.log_stream_manager.streams.get(&id).is_none() {
                self.log_stream_manager.add_stream(id.clone());
            } else {
                let src = self.sources.get_mut(&id).expect("no source state");
                if src.closed {
                    src.log_lines.clear();
                    self.sources.remove(&id);
                    self.messages.retain(|m| m.id != id);
                }
                self.log_stream_manager.drop_stream(&id)?;
            }
        }
        Ok(())
    }

    pub async fn handle_message(&mut self, message: TuiMessage) -> Result<(), Box<dyn Error>> {
        match message {
            TuiMessage::K8SMessage(msg) => match msg {
                LogStreamManagerMessage::Log(log) => {
                    let log = Arc::new(log);
                    self.messages.insert(log.clone());
                    let id = log.id.clone();
                    if let Some(src) = self.sources.get_mut(&id).map(|s| &mut s.log_lines) {
                        src.insert(log);
                    }
                }
                LogStreamManagerMessage::LogSourceUpdated(src) => {
                    if self.sources.get(&src).is_none() {
                        self.sources.insert(src, SourceState::default());
                    }
                }
                LogStreamManagerMessage::LogSourceRemoved(id) => {
                    let mut remove = false;
                    if let Some(src) = self.sources.get_mut(&id) {
                        if src.log_lines.len() > 0 {
                            src.closed = true;
                        } else {
                            remove = true;
                        }
                    }
                    if remove {
                        self.sources.remove(&id);
                        self.messages.retain(|m| m.id != id)
                    }
                }
                LogStreamManagerMessage::LogSourceSubscribed(src) => {}
                LogStreamManagerMessage::LogSourceCancelled(src) => {
                    if let Some(src) = self.sources.get_mut(&src) {
                        src.log_lines.clear();
                    }
                    self.messages.retain(|m| m.id != src)
                }
            },
            TuiMessage::Key(key) => match key.code {
                KeyCode::Up => self.key_up(),
                KeyCode::Char('k') => self.key_up(),
                KeyCode::Down => self.key_down(),
                KeyCode::Char('j') => self.key_down(),
                KeyCode::Enter => self.key_enter()?,
                KeyCode::Esc => {
                    // if we send into the channel in the same loop that handles
                    // messages, and it's full, we hang forever.
                    let tx = self.tx.clone();
                    tokio::spawn(async move {
                        tx.send(TuiMessage::Exit).await.expect("couldn't send exit");
                    });
                }
                KeyCode::Char('q') => {
                    let tx = self.tx.clone();
                    tokio::spawn(async move {
                        tx.send(TuiMessage::Exit).await.expect("couldn't send exit");
                    });
                }
                _ => {}
            },
            _ => return Err(Box::new(TuiError::UnhandleableMessage)),
        }
        Ok(())
    }
}
